/*
 Copyright (C) 2015 - 2017, 2019 - 2020, 2025 3NSoft Inc.
 
 This program is free software: you can redistribute it and/or modify it under
 the terms of the GNU General Public License as published by the Free Software
 Foundation, either version 3 of the License, or (at your option) any later
 version.
 
 This program is distributed in the hope that it will be useful, but
 WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 See the GNU General Public License for more details.
 
 You should have received a copy of the GNU General Public License along with
 this program. If not, see <http://www.gnu.org/licenses/>.
*/

/**
 * Inbox files are laid out on disk in the following way:
 * (a) store is just a folder with stuff inside;
 * (b) main store folder contains following folders:
 * (b.1) messages - is a folder for message folders,
 * (b.2) delivery - is a folder for messages, that are in a process of
 *                  being delivered; complete messages are moved to
 *                  'messages' folder.
 * (b.3) params - is a place for information files about this mail box;
 * (c) message folder's name is message's id. Each message folder contains:
 * (c.1) meta - is a file with plain-text JSON-form metadata for a message.
 * (c.2) objects - is a folder with all object files, that are part of the
 *                 message.
 */

import * as fs from '../../lib-common/async-fs-node';
import { Readable } from 'stream';
import { isLikeSignedKeyCert } from '../../lib-common/jwkeys';
import * as random from '../../lib-common/random-node';
import * as deliveryApi from '../../lib-common/service-api/asmail/delivery';
import * as configApi from '../../lib-common/service-api/asmail/config';
import * as retrievalApi from '../../lib-common/service-api/asmail/retrieval';
import { UserFiles, SC as ufSC, ObjReader } from '../../lib-server/resources/user-files';
import { NamedProcs } from '../../lib-common/processes';
import { TimeWindowCache } from '../../lib-common/time-window-cache';
import { ObjVersionFile } from '../../lib-common/objs-on-disk/obj-file';
import { join } from 'path';
import { errWithCause } from '../../lib-common/exceptions/error';
import { chunksInOrderedStream, streamToObjFile, makeNoBaseObjPipe } from '../../lib-common/objs-on-disk/utils';

export { ObjPipe, ObjReader } from '../../lib-server/resources/user-files';

type InitPubKey = configApi.p.initPubKey.Certs;
type AnonSenderPolicy = configApi.p.anonSenderPolicy.Policy;
export type AuthSenderPolicy = configApi.p.authSenderPolicy.Policy;
type Whitelist = configApi.p.authSenderWhitelist.List;
type Blacklist = configApi.p.authSenderBlacklist.List;
type AuthSenderInvites = configApi.p.authSenderInvites.List;
type AnonSenderInvites = configApi.p.anonSenderInvites.List;

type MsgMeta = retrievalApi.MsgMeta;
export type MsgEvents = retrievalApi.msgMainObjRecieved.Event | retrievalApi.msgRecievedCompletely.Event;

export type MailEventsSink = (
	userId: string, channel: string, event: MsgEvents
) => void;

export const SC = {
	OBJ_EXIST: 'obj-already-exist',
	USER_UNKNOWN: ufSC.USER_UNKNOWN,
	MSG_UNKNOWN: 'msg-unknown',
	OBJ_UNKNOWN: 'obj-unknown',
	WRONG_OBJ_STATE: 'wrong-obj-state',
	OBJ_FILE_INCOMPLETE: 'obj-file-incomplete'
};
Object.freeze(SC);

const META_FILE = 'meta.json';

const MSG_ID_LEN = 32;

/**
 * This returns a promise, resolvable to generated msg id, when folder for a
 * message is created in the delivery folder.
 * @param delivPath
 * @param msgsFolder is an additional folder, checked against id-collision
 */
async function genMsgIdAndMakeFolder(
	delivPath: string, msgsFolder: string
): Promise<string> {
	const msgId = await random.stringOfB64UrlSafeChars(MSG_ID_LEN);
	// make msg folder among deliveries
	try {
		await fs.mkdir(join(delivPath, msgId));
	} catch (exc) {
		if ((<fs.FileException> exc).alreadyExists) {
			return genMsgIdAndMakeFolder(delivPath, msgsFolder);
		} else { throw exc; }
	}
	// ensure that msgId does not belong to any existing message
	try {
		await fs.stat(join(msgsFolder, msgId));
		await fs.rmdir(join(delivPath, msgId));
		return genMsgIdAndMakeFolder(delivPath, msgsFolder);
	} catch (exc) {}
	return msgId;
}

export interface InboxParams {
	"pubkey": InitPubKey|null;
	"anonymous/policy": AnonSenderPolicy;
	"anonymous/invites": AnonSenderInvites;
	"authenticated/policy": AuthSenderPolicy;
	"authenticated/blacklist": Blacklist;
	"authenticated/whitelist": Whitelist;
	"authenticated/invites": AuthSenderInvites;
}


export class Inbox extends UserFiles<InboxParams> {

	private readonly metas = new MsgMetas(5*60*1000, join(this.path, 'delivery'), join(this.path, 'messages'));

	private readonly objFiles = new ObjFiles(5*60*1000, this.metas.deliveryFolder, this.metas.readyMsgsFolder);

	constructor(
		userId: string, path: string,
		private mailEventsSink: MailEventsSink,
		writeBufferSize?: string|number, readBufferSize?: string|number
	) {
		super(userId, path, writeBufferSize, readBufferSize);
		Object.freeze(this);
	}
	
	static async make(
		userFolder: string, userId: string, mailEventsSink: MailEventsSink,
		writeBufferSize?: string|number, readBufferSize?: string|number
	): Promise<Inbox> {
		const path = join(userFolder, 'mail');
		const inbox = new Inbox(userId, path, mailEventsSink, writeBufferSize, readBufferSize);
		await inbox.ensureUserExistsOnDisk();
		return inbox;
	}
	
	/**
	 * @return a promise, resolvable to number bytes used by this inbox.
	 */
	async usedSpace(): Promise<number> {
		// XXX need to use space-tracker service!
		//		For now we return zero.
		return 0;
	}

	/**
	 * @return a promise, resolvable to free space in bytes.
	 */
	async freeSpace(): Promise<number> {
		// XXX need to use space-tracker service!
		const usedSpace = await this.usedSpace();
		const quota = await this.getSpaceQuota();
		return Math.max(0, quota - usedSpace);
	}

	/**
	 * This returns a promise, resolvable to message id, when a folder for new
	 * message has been created.
	 * @param extMeta is json object with message's meta info directly from
	 * sender.
	 * @param authSender is an address of sender, if such was authenticated.
	 * @param invite is an invitation token, if any were used.
	 */
	async recordMsgMeta(
		extMeta: deliveryApi.msgMeta.Request, authSender: string|undefined,
		invite: string|undefined, maxMsgLength: number
	): Promise<string> {
		const msgId = await genMsgIdAndMakeFolder(
			this.metas.deliveryFolder, this.metas.readyMsgsFolder);
		const meta: MsgMeta = {
			extMeta, authSender, invite, maxMsgLength,
			recipient: this.userId,
			deliveryStart: Date.now(),
			objs: {}
		};
		await this.metas.set(msgId, meta);
		return msgId;
	}

	/**
	 * This returns a promise, resolvable to message metadata from disk, when it
	 * as been found on the disk.
	 * Rejected promise may pass a string error code from SC.
	 * @param msgId
	 * @param completeMsg flag, true for complete messages, and false for
	 * incomplete (in-delivery) messages.
	 */
	async getMsgMeta(msgId: string, completeMsg: boolean): Promise<MsgMeta> {
		return this.metas.get(msgId, completeMsg);
	}

	async startSavingObj(
		msgId: string, objId: string, bytes: Readable, bytesLen: number,
		opts: deliveryApi.PutObjFirstQueryOpts
	): Promise<void> {
		// check meta
		const meta = await this.metas.get(msgId, false);
		if (meta.extMeta.objIds.indexOf(objId) < 0) { throw SC.OBJ_UNKNOWN; }
		if (meta.objs[objId]) { throw SC.WRONG_OBJ_STATE; }

		const file = await this.objFiles.forNewFile(msgId, objId);
		const chunks = chunksInOrderedStream(bytesLen, opts.header, 0);
		await streamToObjFile(file, chunks, bytes, this.fileWritingBufferSize);

		// set obj status in meta
		meta.objs[objId] = {
			size: {
				header: opts.header,
				segments: bytesLen - opts.header
			}
		};
		if (opts.last) {
			if (file.isFileComplete()) {
				meta.objs[objId].completed = true;
			} else {
				await this.metas.set(msgId, meta);
				throw SC.OBJ_FILE_INCOMPLETE;
			}
		}
		await this.metas.set(msgId, meta);
	}

	async continueSavingObj(
		msgId: string, objId: string, bytes: Readable, bytesLen: number,
		opts: deliveryApi.PutObjSecondQueryOpts
	): Promise<void> {
		// check obj status
		const meta = await this.metas.get(msgId, false);
		const objStatus = meta.objs[objId];
		if (objStatus) {
			if (objStatus.completed) { throw SC.WRONG_OBJ_STATE; }
		} else {
			if (meta.extMeta.objIds.indexOf(objId) < 0) { throw SC.OBJ_UNKNOWN; }
			else { throw SC.WRONG_OBJ_STATE; }
		}

		const file = await this.objFiles.forExistingFile(msgId, objId, true);
		const chunks = chunksInOrderedStream(bytesLen, undefined, opts.ofs);
		await streamToObjFile(file, chunks, bytes, this.fileWritingBufferSize);

		// update meta
		objStatus.size.segments! += bytesLen;
		if (opts.last) {
			if (file.isFileComplete()) {
				objStatus.completed = true;
			} else {
				await this.metas.set(msgId, meta);
				throw SC.OBJ_FILE_INCOMPLETE;
			}
		}
		await this.metas.set(msgId, meta);
	}
	
	/**
	 * This returns a promise, resolvable, when a message has been moved from
	 * delivery to messages storing folder.
	 * Rejected promise may pass string error code from SC.
	 * @param msgId
	 */
	async completeMsgDelivery(msgId: string): Promise<void> {
		// indicate completion in meta
		const meta = await this.metas.get(msgId, false);
		if (!this.areAllMsgObjsComplete(msgId, meta)) {
			throw SC.OBJ_FILE_INCOMPLETE;
		}
		meta.deliveryCompletion = Date.now();
		await this.metas.set(msgId, meta);

		// move message folder to a place with completed messages
		const srcFolder = join(this.metas.deliveryFolder, msgId);
		const dstFolder = join(this.metas.readyMsgsFolder, msgId);
		await fs.rename(srcFolder, dstFolder);
		this.objFiles.changeToReadingPathsInMsg(msgId);
		
		// raise event about completion of receiving a message
		this.mailEventsSink(this.userId, retrievalApi.msgRecievedCompletely.EVENT_NAME, { msgId });
	}

	private areAllMsgObjsComplete(
		msgId: string, meta: retrievalApi.MsgMeta
	): boolean {
		for (const status of Object.values(meta.objs)) {
			if (!status.completed) { return false; }
		}
		return true;
	}

	/**
	 * @param fromTS 
	 * @param toTS 
	 * @return a promise, resolvable to a list of available message ids.
	 */
	async getMsgIds(fromTS: number|undefined, toTS: number|undefined): Promise<retrievalApi.listMsgs.Reply> {
		// XXX this isn't efficient, and needs sqlite-powered index
		const lst = await fs.readdir(this.metas.readyMsgsFolder);
		if ((fromTS === undefined) && (toTS === undefined)) {
			return lst;
		}
		const filteredLst: string[] = [];
		for (const msgId of lst) {
			try {
				const { deliveryStart } = await this.metas.get(msgId, true);
				if (fromTS && (deliveryStart < fromTS)) {
					continue;
				}
				if (toTS && (toTS < deliveryStart)) {
					continue;
				}
				filteredLst.push(msgId);
			} catch (exc) {}
		}
		return filteredLst;
	}

	/**
	 * This method returns parameters of an incomplete message, identified
	 * by a given id.
	 * @param msgId
	 */
	async getIncompleteMsgParams(
		msgId: string
	): Promise<{ maxMsgLength: number; currentMsgLength: number; }> {
		const msgMeta = await this.metas.get(msgId, false);
		let currentMsgLength = 0;
		for (const obj of Object.values(msgMeta.objs)) {
			currentMsgLength += obj.size.header + obj.size.segments!;
		}
		const maxMsgLength = msgMeta.maxMsgLength;
		return { maxMsgLength, currentMsgLength };
	}

	/**
	 * This method removes message folder from the disk.
	 * @param msgId is an id of a message, that needs to be removed.
	 * @return promise, resolvable when a message folder is removed from
	 * the disk.
	 * Rejected promise may pass string error code from SC.
	 */
	async rmMsg(msgId: string): Promise<void> {
		this.metas.uncache(msgId);
		this.objFiles.uncache(msgId);
		try {
			const msgPath = join(this.metas.readyMsgsFolder, msgId);
			const rmPath = `${msgPath}~remove`;
			await fs.rename(msgPath, rmPath);
			await fs.rmDirWithContent(rmPath);
		} catch (exc) {
			if ((exc as fs.FileException).notFound) {
				throw SC.MSG_UNKNOWN;
			}
			throw exc;
		}
	}

	/**
	 * This method returns promise that resolves to obj reader.
	 * @param msgId
	 * @param objId
	 * @param header is a boolean flag that says if header bytes should be
	 * read
	 * @param segsOffset indicates offset from which segment bytes should be
	 * read
	 * @param segsLimit
	 */
	async getObj(
		msgId: string, objId: string,
		header: boolean, segsOfs: number, segsLimit: number|undefined
	): Promise<ObjReader> {
		const file = await this.objFiles.forExistingFile(msgId, objId);

		// find total segments length
		const segsLen = file.getTotalSegsLen();

		// contain boundary parameters offsetIntoSegs and len for segment bytes
		if (segsLen < segsOfs) {
			segsOfs = segsLen;
		}
		let segBytesToRead: number;
		if (segsLimit === undefined) {
			segBytesToRead = segsLen - segsOfs;
		} else if ((segsOfs+segsLimit) >= segsLen) {
			segBytesToRead = segsLen - segsOfs;
		} else {
			segBytesToRead = segsLimit;
		}

		// construct reader
		let reader: ObjReader;
		if (header) {
			const headerLen = file.getHeaderLen()!;
			reader = {
				len: (segBytesToRead + headerLen),
				segsLen,
				headerLen,
				pipe: makeNoBaseObjPipe(file, true, segsOfs, segBytesToRead)
			};
		} else {
			reader = {
				len: segBytesToRead,
				segsLen,
				pipe: makeNoBaseObjPipe(file, false, segsOfs, segBytesToRead)
			};
		}
		Object.freeze(reader);
		
		return reader;
	}
	
	/**
	 * @param initKeyCerts
	 * @return a promise, resolvable to true, when certs are set, or
	 * resolvable to false, when given certs do not pass sanitization. 
	 */
	async setPubKey(initKeyCerts: InitPubKey|null): Promise<boolean> {
		if ((initKeyCerts === null) || isValidIntroPKeyCerts(initKeyCerts)) {
			await this.setParam('pubkey', initKeyCerts);
			return true;
		} else {
			return false;
		}
	}

	async setAnonSenderPolicy(policy: AnonSenderPolicy): Promise<boolean> {
		if (isValidAnonSenderPolicy(policy)) {
			await this.setParam('anonymous/policy', policy);
			return true;
		} else {
			return false;
		}
	}

	async setAnonSenderInvites(invites: AnonSenderInvites): Promise<boolean> {
		if (isValidListWithMsgMaxSizes(invites)) {
			await this.setParam('anonymous/invites', invites);
			return true;
		} else {
			return false;
		}
	}

	async setAuthSenderPolicy(policy: AuthSenderPolicy): Promise<boolean> {
		if (isValidAuthSenderPolicy(policy)) {
			await this.setParam('authenticated/policy', policy);
			return true;
		} else {
			return false;
		}
	}

	async setAuthSenderBlacklist(lst: Blacklist): Promise<boolean> {
		if (isValidBlacklist(lst)) {
			await this.setParam('authenticated/blacklist', lst);
			return true;
		} else {
			return false;
		}
	}

	async setAuthSenderWhitelist(lst: Whitelist): Promise<boolean> {
		if (isValidListWithMsgMaxSizes(lst)) {
			await this.setParam('authenticated/whitelist', lst);
			return true;
		} else {
			return false;
		}
	}

	async setAuthSenderInvites(invites: AuthSenderInvites): Promise<boolean> {
		if (isValidListWithMsgMaxSizes(invites)) {
			await this.setParam('authenticated/invites', invites);
			return true;
		} else {
			return false;
		}
	}

}
Object.freeze(Inbox.prototype);
Object.freeze(Inbox);


function isValidIntroPKeyCerts(pkeyCerts: InitPubKey): boolean {
	return (
		(typeof pkeyCerts === 'object') && !!pkeyCerts &&
		isLikeSignedKeyCert(pkeyCerts.pkeyCert) &&
		isLikeSignedKeyCert(pkeyCerts.userCert) &&
		isLikeSignedKeyCert(pkeyCerts.provCert)
	);
}

function isValidAnonSenderPolicy(policy: AnonSenderPolicy): boolean {
	return (
		('object' === typeof policy) && !!policy &&
		('boolean' === typeof policy.accept) &&
		('boolean' === typeof policy.acceptWithInvitesOnly) &&
		('number' === typeof policy.defaultMsgSize) &&
		(policy.defaultMsgSize > 500)
	);
}

function isValidAuthSenderPolicy(policy: AuthSenderPolicy): boolean {
	return (
		('object' === typeof policy) && !!policy &&
		('boolean' === typeof policy.applyBlackList) &&
		('boolean' === typeof policy.acceptFromWhiteListOnly) &&
		('boolean' === typeof policy.acceptWithInvitesOnly) &&
		('number' === typeof policy.defaultMsgSize) &&
		(policy.defaultMsgSize > 500)
	);
}

function isValidListWithMsgMaxSizes(
	lst: AuthSenderInvites|AnonSenderInvites|Whitelist
): boolean {
	const isOK = ('object' === typeof lst) && !!lst;
	if (!isOK) { return false; }
	for (const inviteOrAddr in lst) {
		const msgMaxSize = lst[inviteOrAddr];
		const isOK = ('number' === typeof msgMaxSize) && (msgMaxSize > 500);
		if (!isOK) { return false; }
	}
	return true;
}

function isValidBlacklist(lst: Blacklist): boolean {
	return ('object' === typeof lst) && !!lst;
}


class ObjFiles {

	private cache: TimeWindowCache<string, Map<string, ObjVersionFile>>;

	constructor(
		cachePeriodMillis: number,
		public deliveryFolder: string,
		public readyMsgsFolder: string
	) {
		this.cache = new TimeWindowCache(cachePeriodMillis);
		Object.freeze(this);
	}

	private getCached(msgId: string, objId: string): ObjVersionFile|undefined {
		const msgObjs = this.cache.get(msgId);
		return (msgObjs ? msgObjs.get(objId) : undefined);
	}

	private setIntoCache(
		msgId: string, objId: string, obj: ObjVersionFile
	): void {
		let msgObjs = this.cache.get(msgId);
		if (!msgObjs) {
			msgObjs = new Map();
			this.cache.set(msgId, msgObjs);
		}
		msgObjs.set(objId, obj);
	}

	async forNewFile(
		msgId: string, objId: string
	): Promise<ObjVersionFile> {
		const path = this.objFileWritingPath(msgId, objId);
		const objFile = await ObjVersionFile.createNew(path)
		.catch((exc: fs.FileException) => {
			if (exc.alreadyExists) { throw SC.OBJ_EXIST; }
			throw exc;
		});
		this.setIntoCache(msgId, objId, objFile);
		return objFile;
	}

	async forExistingFile(
		msgId: string, objId: string, forWriting = false
	): Promise<ObjVersionFile> {
		let objFile = this.getCached(msgId, objId);
		if (!objFile) {
			const path = (forWriting ?
				this.objFileWritingPath(msgId, objId) :
				this.objFileReadingPath(msgId, objId));
			objFile = await ObjVersionFile.forExisting(path)
			.catch((exc: fs.FileException) => {
				if (exc.notFound) { throw SC.OBJ_UNKNOWN; }
				throw exc;
			});
			this.setIntoCache(msgId, objId, objFile);
		}
		return objFile;
	}

	private objFileWritingPath(msgId: string, objId: string): string {
		return join(this.deliveryFolder, msgId, objId);
	}

	private objFileReadingPath(msgId: string, objId: string): string {
		return join(this.readyMsgsFolder, msgId, objId);
	}

	changeToReadingPathsInMsg(msgId: string): void {
		const msgObjs = this.cache.get(msgId);
		if (!msgObjs) { return; }
		for (const [ objId, obj ] of msgObjs.entries()) {
			obj.changePathWithoutFileMove(this.objFileReadingPath(msgId, objId));
		}
	}

	uncache(msgId: string): void {
		this.cache.delete(msgId);
	}

}
Object.freeze(ObjFiles.prototype);
Object.freeze(ObjFiles);

class MsgMetas {

	private cache: TimeWindowCache<string|null, MsgMeta>;
	private saveProcs = new NamedProcs();

	constructor(
		cachePeriodMillis: number,
		public deliveryFolder: string,
		public readyMsgsFolder: string
	) {
		this.cache = new TimeWindowCache(cachePeriodMillis);
		Object.freeze(this);
	}

	async get(msgId: string, completeMsg: boolean): Promise<MsgMeta> {
		let meta = this.cache.get(msgId);
		if (meta) {
			if ((meta.deliveryCompletion && !completeMsg)
			|| (!meta.deliveryCompletion && completeMsg)) {
				throw SC.MSG_UNKNOWN;
			}
		} else {
			meta = await this.fromFile(msgId, completeMsg);
			this.cache.set(msgId, meta);
		}
		return meta;
	}

	private async fromFile(
		msgId: string, completeMsg: boolean
	): Promise<MsgMeta> {
		const filePath = join(
			(completeMsg ? this.readyMsgsFolder : this.deliveryFolder),
			msgId, META_FILE);
		const str = await fs.readFile(filePath, { encoding: 'utf8', flag: 'r' })
		.catch((exc: fs.FileException) => {
			if (exc.notFound) {
				throw SC.MSG_UNKNOWN;
			}
			throw exc;
		});
		try {
			return JSON.parse(str) as MsgMeta;
		} catch (err) {
			throw errWithCause(err, `Can't parse content of message meta file`);
		}
	}

	async set(msgId: string, meta: MsgMeta): Promise<void> {
		this.cache.set(msgId, meta);
		await this.saveToFile(msgId, meta);
	}

	private async saveToFile(msgId: string, meta: MsgMeta): Promise<void> {
		const filePath = join(this.deliveryFolder, msgId, META_FILE);
		await this.saveProcs.startOrChain(msgId, () => fs.writeFile(
			filePath, JSON.stringify(meta), { encoding: 'utf8', flag: 'w' }));
	}

	uncache(msgId: string): void {
		this.cache.delete(msgId);
	}

}
Object.freeze(MsgMetas.prototype);
Object.freeze(MsgMetas);


Object.freeze(exports);