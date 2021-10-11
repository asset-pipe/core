'use strict';

const { validators } = require('@eik/common');
const originalUrl = require('original-url');
const HttpError = require('http-errors');
const abslog = require('abslog');
const Metrics = require('@metrics/client');

const {
    createFilePathToAlias,
    createFilePathToAliasOrigin,
} = require('../utils/path-builders-fs');
const { createURIToAlias } = require('../utils/path-builders-uri');
const MultipartParser = require('../multipart/parser');
const HttpIncoming = require('../classes/http-incoming');
const HttpOutgoing = require('../classes/http-outgoing');
const Author = require('../classes/author');
const Alias = require('../classes/alias');
const config = require('../utils/defaults');
const utils = require('../utils/utils');

const AliasPost = class AliasPost {
    constructor({
        organizations,
        cacheControl,
        logger,
        sink,
    } = {}) {
        this._organizations = organizations || config.organizations;
        this._cacheControl = cacheControl;
        this._sink = sink;
        this._log = abslog(logger);
        this._metrics = new Metrics();
        this._histogram = this._metrics.histogram({
            name: 'eik_core_alias_post_handler',
            description:
                'Histogram measuring time taken in @eik/core AliasPost handler method',
            labels: {
                success: true,
                type: 'unknown',
            },
            buckets: [
                0.005,
                0.01,
                0.06,
                0.1,
                0.6,
                1.0,
                2.0,
                4.0,
            ],
        });
        this._orgRegistry = new Map(this._organizations);

        this._multipart = new MultipartParser({
            pkgMaxFileSize: this._pkgMaxFileSize,
            legalFields: ['version'],
            sink: this._sink,
        });
    }

    get metrics() {
        return this._metrics;
    }

    _parser(incoming) {
        return new Promise((resolve, reject) => {
            this._multipart
                .parse(incoming)
                .then((result) => {
                    const obj = result[0];
                    if (obj && obj.constructor.name === 'FormField') {
                        try {
                            validators.version(obj.value);
                        } catch (error) {
                            this._log.error(
                                `alias:post - Input field could not be validated - Name: ${obj.name} - Value: ${obj.value}`,
                            );
                            this._log.trace(error);
                            throw new HttpError.BadRequest();
                        }
                        const alias = new Alias(incoming);
                        alias.version = obj.value;
                        return alias;
                    }

                    throw new HttpError.BadRequest();
                })
                .then(async (alias) => {
                    try {
                        const path = createFilePathToAliasOrigin({
                            org: alias.org,
                            type: alias.type,
                            name: alias.name,
                            version: alias.version,
                        });
                        await this._sink.read(path)
                    } catch (error) {
                        this._log.error(
                            `alias:post - Unable to locate requested published package version - Version ${alias.name}@${alias.version}`,
                        );
                        this._log.trace(error);
                        throw new HttpError.NotFound();
                    }
                    return alias;
                })
                .then(async (alias) => {
                    const path = createFilePathToAlias(incoming);

                    this._log.info(
                        `alias:post - Start writing alias to sink - Pathname: ${path}`,
                    );

                    try {
                        await utils.writeJSON(
                            this._sink,
                            path,
                            alias,
                            'application/json',
                        );
                    } catch (error) {
                        this._log.error(
                            `alias:post - Failed writing alias to sink - Pathname: ${path}`,
                        );
                        this._log.trace(error);
                        throw new HttpError.BadGateway();
                    }

                    this._log.info(
                        `alias:post - Successfully wrote alias to sink - Pathname: ${path}`,
                    );

                    resolve(alias);
                })
                .catch((error) => {
                    reject(error);
                });
        });
    }


    async _exist (incoming) {
        try {
            const path = createFilePathToAlias(incoming);
            await this._sink.exist(path);
            return true;
        } catch (error) {
            return false;
        }
    }

    async handler(req, user, type, name, alias) {
        const end = this._histogram.timer();

        const pAlias = utils.decodeUriComponent(alias);
        const pName = utils.decodeUriComponent(name);

        try {
            validators.alias(pAlias);
            validators.name(pName);
            validators.type(type);
        } catch (error) {
            this._log.info(`alias:post - Validation failed - ${error.message}`);
            const e = new HttpError.NotFound();
            end({ labels: { success: false, status: e.status } });
            throw e;
        }

        const url = originalUrl(req);
        const org = this._orgRegistry.get(url.hostname);

        if (!org) {
            this._log.info(`alias:post - Hostname does not match a configured organization - ${url.hostname}`);
            const e = new HttpError.InternalServerError();
            end({ labels: { success: false, status: e.status, type } });
            throw e;
        }

        const author = new Author(user);

        const incoming = new HttpIncoming(req, {
            author,
            alias: pAlias,
            name: pName,
            type,
            org,
        });

        const exist = await this._exist(incoming);
        if (!exist) {
            this._log.info(
                `alias:post - Alias does not exists - Org: ${org} - Type: ${type} - Name: ${pName} - Alias: ${pAlias}`,
            );
            const e = new HttpError.NotFound();
            end({ labels: { success: false, status: e.status, type } });
            throw e;
        }

        await this._parser(incoming);

        const outgoing = new HttpOutgoing();
        outgoing.cacheControl = this._cacheControl;
        outgoing.statusCode = 303;
        outgoing.location = createURIToAlias(incoming);

        end({ labels: { status: outgoing.statusCode, type } });

        return outgoing;
    }
};
module.exports = AliasPost;
