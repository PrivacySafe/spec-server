/*
 Copyright (C) 2017, 2019 3NSoft Inc.
 
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

import * as WebSocket from 'ws';
import { IncomingMessage } from 'http';
import { SESSION_ID_HEADER, Reply } from './xhr-utils';
import { defer } from '../../lib-common/processes';
import { makeConnectionException } from '../../lib-common/exceptions/http';
import { globalAgent as agent } from 'https';

export function openSocket(url: string, sessionId: string): Promise<Reply<WebSocket>> {
	if (!url.startsWith('wss://')) { throw new Error(
		`Url protocol must be wss`); }
	const headers: any = {};
	headers[SESSION_ID_HEADER] = sessionId;
	const ws = new WebSocket(url, { headers, agent });
	const opening = defer<Reply<WebSocket>>();
	const initOnError = err => opening.reject(makeConnectionException(
		url, undefined,
		`Cannot open websocket connection due to error: ${err.message}`));
	const onNonOkReply = (req, res: IncomingMessage) => {
		const errReply: Reply<WebSocket> = {
			url,
			method: 'GET',
			status: res.statusCode!,
			data: (undefined as any)
		};
		opening.resolve(errReply);
	};
	ws.on('error', initOnError);
	ws.on('unexpected-response', onNonOkReply);
	ws.once('open', () => {
		const okReply: Reply<WebSocket> = {
			url,
			method: 'GET',
			status: 200,
			data: ws
		};
		opening.resolve(okReply);
		ws.removeListener('error', initOnError);
		ws.removeListener('unexpected-response', onNonOkReply);
	});
	return opening.promise;
}

Object.freeze(exports);