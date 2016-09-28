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

/**
 * This module gives a function that creates a mountable, or app.use()-able,
 * express 3NStorage sharing application.
 */

import * as express from 'express';

// Internal libs
import { json as parseJSON } from '../lib-server/middleware/body-parsers'
import { allowCrossDomain } from '../lib-server/middleware/allow-cross-domain';

// Resource/Data modules
import { Factory as sessionsFactory } from '../lib-server/resources/sessions';
import { Factory as usersFactory } from './resources/users';

// routes


//import * as api from '../../lib-common/service-api/3nstorage/shared';

export function makeApp(sessions: sessionsFactory, users: usersFactory):
		express.Express {
	
	let app = express();
	app.disable('etag');
	
	app.use(allowCrossDomain(
			[ "Content-Type", "X-Session-Id" ],
			[ 'GET', 'POST', 'PUT', 'DELETE' ]));
	
	// TODO add sharing routes with proper session creation for authorities/capabilities
	// If capacity needs only PKL entry, then manipulations can be done anonymously.
	// If in addition to PKL, MailerId is required, then it sort of let's server
	//  protect against loosing capabilities by user's peers, but at a cost of
	//  anonymity.
	// Should we allow MailerId-only authenticating capabilities? No,
	//  since sharing peer will anyway need to have a file to store long names
	//  and file keys.
	
	return app;
}

Object.freeze(exports);