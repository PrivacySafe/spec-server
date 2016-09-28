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
 this program. If not, see <http://www.gnu.org/licenses/>. */

import { RequestHandler, Response, NextFunction } from 'express';
import { SC as recipSC } from '../../resources/recipients';
import { PARAM_SC, ERR_SC }
	from '../../../lib-common/service-api/asmail/config';
import { Request } from '../../../lib-server/routes/sessions/start';

export function getParam<T>(paramGetter: (userId: string) => Promise<T>):
		RequestHandler {
	
	if ('function' !== typeof paramGetter) { throw new TypeError(
			"Given argument 'paramGetter' must be function, but is not."); }
	
	return async function(req: Request, res: Response, next: NextFunction) {
		
		let session = req.session;
		let userId = session.params.userId;
		
		try{
			let value = await paramGetter(userId);
			res.status(PARAM_SC.ok).json(value);
		} catch (err) {
			if (err === recipSC.USER_UNKNOWN) {
				res.status(ERR_SC.server).send(
					"Recipient disappeared from the system.");
				session.close();
			} else {
				next(err);
			}
		}
		
	};
	
}
Object.freeze(exports);