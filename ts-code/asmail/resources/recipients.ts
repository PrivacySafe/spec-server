/*
 Copyright (C) 2015 - 2017, 2020, 2025 3NSoft Inc.
 
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

/*
 * This module is recipient boxes factory.
 */

import { Readable } from 'stream';
import { Inbox, ObjReader, AuthSenderPolicy, SC, MailEventsSink, InboxParams } from './inbox';
import * as deliveryApi from '../../lib-common/service-api/asmail/delivery';
import * as retrievalApi from '../../lib-common/service-api/asmail/retrieval';
import { userDataInRootFolder } from '../../lib-server/resources/server-data-folders';

export { SC } from './inbox';

export const BIN_TYPE = 'application/octet-stream';

interface AddressToSizeMap {
	[address: string]: number;
}

/**
 * @param lst is a map from addresses to numeric values
 * @param address
 * @return numeric value found in the list, or undefined,
 * if neither address, nor its domain can be matched in the list.
 */
function findMatchIn(lst: AddressToSizeMap, address: string): number|undefined {
	// check address as a whole
	let v = lst[address];
	if ('undefined' !== typeof v) { return v; }
	// check address' own domain
	const ind = address.indexOf('@');
	if (ind < 0) { return; }
	address = address.substring(ind+1);
	if (address.length === 0) { return; }
	v = lst['@'+address];
	if ('undefined' !== typeof v) { return v; }
	// check parent domains
	while (true) {
		const ind = address.indexOf('.');
		if (ind < 0) { return; }
		address = address.substring(ind+1);
		if (address.length === 0) { return; }
		v = lst['@*.'+address];
		if ('undefined' !== typeof v) { return v; }
	}
}
	
/**
 * @param inbox
 * @param msgSize is a number of message bytes
 * @returns a promise, resolvable to
 * (1) least number between given number of bytes, and free space of
 *     a given inbox;
 * (2) -1 (less than zero), if there is no free space in the inbox.
 */
async function adaptToFreeSpaceLeft(
	inbox: Inbox, msgSize: number
): Promise<number> {
	const bytesFree = await inbox.freeSpace();
	return ((bytesFree > 0) ? Math.min(bytesFree, msgSize) : -1);
}

/**
 * @param inbox
 * @param invitation is a string invitation token, or undefined.
 * @returns a promise, resolvable to
 * (1) zero (0), if leaving mail is forbidden,
 * (2) greater than zero maximum message length, and
 * (3) -1 (less than zero), if mail cannot be accepted due to full
 *     mail box.
 */
async function allowedMsgSizeForAnonSender(
	inbox: Inbox, invitation: string|undefined
): Promise<number> {
	const policy = await inbox.getParam('anonymous/policy');
	if (policy.accept) {

		// XXX this misses case when invite allows for bigger messages,
		//     while without invite messages are also allowed.

		if (policy.acceptWithInvitesOnly) {
			if (invitation) {
				const invites = await inbox.getParam('anonymous/invites');
				const sizeForInvite = invites[invitation];
				return ((typeof sizeForInvite === 'number') ?
					adaptToFreeSpaceLeft(inbox, sizeForInvite) : 0
				);
			} else {
				return 0;
			}
		} else {
			return await adaptToFreeSpaceLeft(inbox, policy.defaultMsgSize);
		}
	} else {
		return 0;
	}
}

/**
 * @param inbox
 * @param sender is sender string address
 * @param invitation is a string invitation token, or undefined.
 * @returns a promise, resolvable to
 * (1) zero (0), if leaving mail is forbidden,
 * (2) greater than zero maximum message length, and
 * (3) -1 (less than zero), if mail cannot be accepted due to full mail
 *     box.
 */
async function allowedMsgSizeForAuthSender(
	inbox: Inbox, sender: string, invitation: string|undefined
): Promise<number> {
	const results = await Promise.all<any>([
		inbox.getParam('authenticated/policy'),
		inbox.getParam('authenticated/whitelist')
	])
	const policy: AuthSenderPolicy = results[0];
	const sizeFromWL = findMatchIn(<AddressToSizeMap> results[1], sender);
	// check whitelist for specific size
	if (typeof sizeFromWL === 'number') {
		return adaptToFreeSpaceLeft(inbox, sizeFromWL);
	} else if (typeof sizeFromWL !== 'undefined') {
		return adaptToFreeSpaceLeft(inbox, policy.defaultMsgSize);
	}
	// exit if only whitelist contacts are allowed
	if (policy.acceptFromWhiteListOnly) { return 0; }
	// if needed, apply blacklist
	if (policy.applyBlackList) {
		const bList = await inbox.getParam('authenticated/blacklist');
		if (typeof findMatchIn(bList, sender) === 'undefined') {
			return adaptToFreeSpaceLeft(inbox, policy.defaultMsgSize);
		} else {
			return 0;
		}
	}
	return adaptToFreeSpaceLeft(inbox, policy.defaultMsgSize);
}

export type GetParam<P extends keyof InboxParams> = (
	userId: string
) => Promise<InboxParams[P]>;
export type SetParam<P extends keyof InboxParams> = (
	userId: string, param: InboxParams[P]
) => Promise<boolean>;

export type EventsSink = MailEventsSink;

export interface Recipients {
	config: ASMailServiceConfig;
	delivery: MsgDelivery;
	retrieval: MsgRetrieval;
}

export interface ASMailServiceConfig {

	/**
	 * This checks existence of a given user, returning a promise, resolvable
	 * either to true, when given user id is known, or to false, when it is not.
	 */
	exists: (userId: string) => Promise<boolean>;

	getPubKey: GetParam<'pubkey'>;
	setPubKey: SetParam<'pubkey'>;

	getAnonSenderPolicy: GetParam<'anonymous/policy'>;
	setAnonSenderPolicy: SetParam<'anonymous/policy'>;

	getAnonSenderInvites: GetParam<'anonymous/invites'>;
	setAnonSenderInvites: SetParam<'anonymous/invites'>;

	getAuthSenderPolicy: GetParam<'authenticated/policy'>;
	setAuthSenderPolicy: SetParam<'authenticated/policy'>;

	getAuthSenderInvites: GetParam<'authenticated/invites'>;
	setAuthSenderInvites: SetParam<'authenticated/invites'>;

	getAuthSenderBlacklist: GetParam<'authenticated/blacklist'>;
	setAuthSenderBlacklist: SetParam<'authenticated/blacklist'>;

	getAuthSenderWhitelist: GetParam<'authenticated/whitelist'>;
	setAuthSenderWhitelist: SetParam<'authenticated/whitelist'>;

}

export interface MsgDelivery {

	getPubKey: ASMailServiceConfig['getPubKey'];

	/**
	 * This tells what is an allowable maximum message size for a given
	 * recipient, for a given sender and/or under a given invitation token.
	 * Function returns a promise, resolvable to
	 * (1) zero (0), if leaving mail is forbidden,
	 * (2) greater than zero maximum message length, and
	 * (3) -1 (less than zero), if mail cannot be accepted due to full mail
	 *     box.
	 */
	allowedMaxMsgSize: (
		recipient: string, sender: string|undefined, invitation: string|undefined
	) => Promise<number>;

	/**
	 * This allocates storage for a message returning a promise, resolvable to
	 * (1) message id, when a folder for new message has been created,
	 * (2) undefined, if recipient is unknown.
	 */
	setMsgStorage: (
		recipient: string, msgMeta: deliveryApi.msgMeta.Request,
		authSender: string|undefined, invite: string|undefined, maxMsgLength: number
	) => Promise<string>;

	/**
	 * This saves object's bytes, returning a promise, resolvable when saving
	 * is OK, otherwise, promise rejects with string error code from SC.
	 */
	saveObj: (
		recipient: string, msgId: string, objId: string,
		fstReq: deliveryApi.PutObjFirstQueryOpts|undefined,
		sndReq: deliveryApi.PutObjSecondQueryOpts|undefined,
		bytesLen: number, bytes: Readable
	) => Promise<void>;

	/**
	 * This returns parameter of a message that is still in delivery, identified
	 * by given recipient and message id. If message is unknown, or if it has
	 * already been delivered, error code is thrown.
	 * Rejected promise may have a string error code from SC.
	 */
	incompleteMsgDeliveryParams: (
		recipient: string, msgId: string
	) => Promise<{ maxMsgLength: number; currentMsgLength: number; }>;

	/**
	 * This finalizes delivery of a message, returning a promise.
	 * Rejected promise may have a string error code from SC.
	 */
	finalizeDelivery: (
		recipient: string, msgId: string
	) => Promise<void>;

}

export interface MsgRetrieval {

	exists: ASMailServiceConfig['exists'];

	/**
	 * This returns a promise, resolvable to array with ids of available
	 * messages. Rejected promise may have a string error code from SC.
	 */
	getMsgIds: (
		userId: string, fromTS: number|undefined, toTS: number|undefined
	) => Promise<retrievalApi.listMsgs.Reply>;

	/**
	 * This returns a promise, resolvable to message meta.
	 * Rejected promise may have a string error code from SC.
	 */
	getMsgMeta: (userId: string, msgId: string) => Promise<retrievalApi.MsgMeta>;

	/**
	 * This deletes a message returning a promise, resolvable when message is
	 * removed.
	 * Rejected promise may have a string error code from SC.
	 */
	deleteMsg: (userId: string, msgId: string) => Promise<void>;

	/**
	 * This returns a promise, resolvable to readable stream of object bytes.
	 * Rejected promise may be passing string error code from SC.
	 */
	getObj: (
		userId: string, msgId: string, objId: string,
		header: boolean, segsOffset: number, segsLimit: number|undefined
	) => Promise<ObjReader>;

	setMailEventsSink(sink: EventsSink): void;

}

export function makeRecipients(
	rootFolder: string,
	writeBufferSize?: string|number, readBufferSize?: string|number
): Recipients {

	const boxes = new Map<string, Inbox>();

	let mailEventsSink: EventsSink|undefined = undefined;

	async function getInbox(userId: string): Promise<Inbox> {
		if (!mailEventsSink) {
			throw new Error(`Mail events sink is not set`);
		}
		let inbox = boxes.get(userId);
		if (inbox) {
			try {
				await inbox.ensureUserExistsOnDisk();
				return inbox;
			} catch (err) {
				boxes.delete(userId);
				throw err;
			}
		} else {
			const userFolder = userDataInRootFolder(rootFolder, userId);
			inbox = await Inbox.make(userFolder, userId, mailEventsSink, writeBufferSize, readBufferSize);
			boxes.set(userId, inbox);
			return inbox;
		}
	}

	const config: ASMailServiceConfig = {

		exists: async userId => {
			try {
				await getInbox(userId);
				return true;
			} catch (err) {
				if (err !==  SC.USER_UNKNOWN) { throw err; }
				return false;
			}
		},

		getPubKey: async recipient => {
			const inbox = await getInbox(recipient);
			return inbox.getParam('pubkey');
		},
		setPubKey: async (userId, pkey) => {
			const inbox = await getInbox(userId);
			return inbox.setPubKey(pkey);
		},

		getAnonSenderPolicy: async recipient => {
			const inbox = await getInbox(recipient);
			return inbox.getParam('anonymous/policy');
		},
		setAnonSenderPolicy: async (userId, policy) => {
			const inbox = await getInbox(userId);
			return inbox.setAnonSenderPolicy(policy);
		},

		getAnonSenderInvites: async recipient => {
			const inbox = await getInbox(recipient);
			return inbox.getParam('anonymous/invites');
		},
		setAnonSenderInvites: async (userId, invites) => {
			const inbox = await getInbox(userId);
			return inbox.setAnonSenderInvites(invites);
		},

		getAuthSenderPolicy: async recipient => {
			const inbox = await getInbox(recipient);
			return inbox.getParam('authenticated/policy');
		},
		setAuthSenderPolicy: async (userId, policy) => {
			const inbox = await getInbox(userId);
			return inbox.setAuthSenderPolicy(policy);
		},
	
		getAuthSenderInvites: async recipient => {
			const inbox = await getInbox(recipient);
			return inbox.getParam('authenticated/invites');
		},
		setAuthSenderInvites: async (userId, invites) => {
			const inbox = await getInbox(userId);
			return inbox.setAnonSenderInvites(invites);
		},
	
		getAuthSenderBlacklist: async recipient => {
			const inbox = await getInbox(recipient);
			return inbox.getParam('authenticated/blacklist');
		},
		setAuthSenderBlacklist: async (userId, list) => {
			const inbox = await getInbox(userId);
			return inbox.setAuthSenderBlacklist(list);
		},
	
		getAuthSenderWhitelist: async recipient => {
			const inbox = await getInbox(recipient);
			return inbox.getParam('authenticated/whitelist');
		},
		setAuthSenderWhitelist: async (userId, list) => {
			const inbox = await getInbox(userId);
			return inbox.setAuthSenderWhitelist(list);
		}

	};
	Object.freeze(config);

	const delivery: MsgDelivery = {

		getPubKey: config.getPubKey,

		allowedMaxMsgSize: async (recipient, sender, invitation) => {
			const inbox = await getInbox(recipient);
			// XXX move these two functions into inbox
			if (sender) {
				return allowedMsgSizeForAuthSender(inbox, sender, invitation);
			} else {
				return allowedMsgSizeForAnonSender(inbox, invitation);
			}
		},

		setMsgStorage: async (
			recipient, msgMeta, authSender, invite, maxMsgLength
		) => {
			const inbox = await getInbox(recipient);
			return inbox.recordMsgMeta(msgMeta, authSender, invite, maxMsgLength);
		},

		saveObj: async (
			recipient, msgId, objId, fstReq, sndReq, bytesLen, bytes
		) => {
			const inbox = await getInbox(recipient);
			if (fstReq) {
				return inbox.startSavingObj(msgId, objId, bytes, bytesLen, fstReq);
			} else if (sndReq) {
				return inbox.continueSavingObj(msgId, objId, bytes, bytesLen, sndReq);
			} else {
				throw new Error(`Missing both request options`);
			}
		},

		finalizeDelivery: async (recipient, msgId) => {
			const inbox = await getInbox(recipient);
			return inbox.completeMsgDelivery(msgId);
		},

		incompleteMsgDeliveryParams: async (recipient, msgId) => {
			const inbox = await getInbox(recipient);
			return inbox.getIncompleteMsgParams(msgId);
		}

	};
	Object.freeze(delivery);

	const retrieval: MsgRetrieval = {

		exists: config.exists,

		getMsgIds: async (userId, fromTS, toTS) => {
			const inbox = await getInbox(userId);
			return inbox.getMsgIds(fromTS, toTS);
		},

		getMsgMeta: async (userId, msgId) => {
			const inbox = await getInbox(userId);
			return inbox.getMsgMeta(msgId, true);
		},

		deleteMsg: async (userId, msgId) => {
			const inbox = await getInbox(userId);
			return inbox.rmMsg(msgId);
		},

		getObj: async (userId, msgId, objId, header, segsOffset, segsLimit) => {
			const inbox = await getInbox(userId);
			return inbox.getObj(msgId, objId, header, segsOffset, segsLimit);
		},

		setMailEventsSink(sink: EventsSink): void {
			if (mailEventsSink) {
				throw new Error(`Mail events sink is already set`);
			}
			mailEventsSink = sink;
		}

	};
	Object.freeze(retrieval);

	const recipients: Recipients = {
		config, delivery, retrieval
	};
	Object.freeze(recipients);

	return recipients;
}

Object.freeze(exports);