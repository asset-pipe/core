'use strict';

const HttpOutgoing = require('../classes/http-outgoing');
const validators = require('../utils/validators');
const httpError = require('http-errors');
const Asset = require('../classes/asset');

const handler = async (sink, req, org, name, version, extra) => {
    try {
        validators.version(version);
        validators.extra(extra);
        validators.name(name);
        validators.org(org);
    } catch (error) {
        throw new httpError(404, 'Not found');
    }

    const asset = new Asset({
        version,
        extra,
        name,
        org,
    });

    try {
        await sink.exist(asset.path);
    } catch (error) {
        throw new httpError(404, 'Not found');
    }

    const outgoing = new HttpOutgoing();
    outgoing.mimeType = asset.mimeType;

    try {
        const stream = await sink.read(asset.path);
        stream.pipe(outgoing);
        return outgoing;
    } catch (error) {
        throw new httpError(500, 'Internal server error');
    }
};
module.exports.handler = handler;