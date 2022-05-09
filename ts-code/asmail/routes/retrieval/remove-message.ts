/*
 Copyright (C) 2015 - 2016 3NSoft Inc.
 
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

import { RequestHandler } from 'express';
import { SC as recipSC, DeleteMsg } from '../../resources/recipients';
import { rmMsg as api, ERR_SC } from '../../../lib-common/service-api/asmail/retrieval';
import { Request } from '../../resources/sessions';

export function deleteMsg(delMsgFunc: DeleteMsg): RequestHandler {
	if ('function' !== typeof delMsgFunc) { throw new TypeError(
			"Given argument 'delMsgFunc' must be function, but is not."); }

	return async (req: Request, res, next) => {
		const userId = req.session.params.userId;
		const msgId: string = req.params.msgId;
		
		try {
			await delMsgFunc(userId, msgId);
			res.status(api.SC.ok).end();
		} catch (err) {
			if ("string" !== typeof err) {
				next(err);
			} else if (err === recipSC.MSG_UNKNOWN) {
				res.status(api.SC.unknownMsg).send(
					"Message "+msgId+" is unknown.");
			} else if (err === recipSC.USER_UNKNOWN) {
				res.status(ERR_SC.server).send(
					"Recipient disappeared from the system.");
				req.session.close();
			} else {
				next(new Error("Unhandled storage error code: "+err));
			}
		}
		
	};
}

Object.freeze(exports);