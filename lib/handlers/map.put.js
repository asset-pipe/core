import validators from '@eik/common-validators';
import originalUrl from 'original-url';
import HttpError from 'http-errors';
import Busboy from 'busboy';
import crypto from 'node:crypto';
import abslog from 'abslog';
import Metrics from '@metrics/client';

import {
    createFilePathToImportMap,
    createFilePathToVersion,
} from '../utils/path-builders-fs.js';
import { createURIPathToImportMap } from '../utils/path-builders-uri.js';
import HttpIncoming from '../classes/http-incoming.js';
import HttpOutgoing from '../classes/http-outgoing.js';
import Versions from '../classes/versions.js';
import Author from '../classes/author.js';
import config from '../utils/defaults.js';
import { decodeUriComponent, streamCollector, writeJSON, readJSON, } from '../utils/utils.js';

const MapPut = class MapPut {
    constructor({
        mapMaxFileSize,
        organizations,
        cacheControl,
        logger,
        sink,
    } = {}) {
        this._mapMaxFileSize = mapMaxFileSize || config.mapMaxFileSize;
        this._organizations = organizations || config.organizations;
        this._cacheControl = cacheControl;
        this._sink = sink;
        this._log = abslog(logger);
        this._metrics = new Metrics();
        this._histogram = this._metrics.histogram({
            name: 'eik_core_map_put_handler',
            description:
                'Histogram measuring time taken in @eik/core MapPut handler method',
            labels: {
                success: true,
                type: 'unknown'
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
    }

    get metrics() {
        return this._metrics;
    }

    _parser(incoming) {
        return new Promise((resolve, reject) => {
            const path = createFilePathToImportMap(incoming);
            const queue = [];

            const busboy = Busboy({
                headers: incoming.headers,
                limits: {
                    fields: 0,
                    files: 1,
                    fileSize: this._mapMaxFileSize,
                },
            });

            busboy.on('file', (fieldname, file) => {
                queue.push(this._handleFile({
                    fieldname,
                    file,
                    path,
                }));
            });

            busboy.on('close', () => {
                Promise.all(queue).then((items) => {
                    // Resolve with only the first item in the array since
                    // only one uploaded file is accepted
                    resolve(items[0]);
                }).catch(error => {
                    reject(error);
                });
            });

            busboy.on('error', error => {
                reject(error);
            });

            // If incoming.request is handeled by stream.pipeline, it will
            // close to early for the http framework to handle it. Let the
            // http framework handle closing incoming.request
            incoming.request.pipe(busboy);
        });
    }

    async _handleFile ({ fieldname, file, path }) {
        // We accept only one file on this given fieldname.
        // Throw if any other files is posted.
        if (fieldname !== 'map') {
            this._log.info(`map:put - Import map submitted on wrong field name - Field: ${fieldname}`);
            throw new HttpError.BadRequest();
        }

        const hasher = crypto.createHash('sha512');

        // Buffer up the incoming file and check if we can
        // parse it as JSON or not.
        let obj = {};
        try {
            const str = await streamCollector(file);
            hasher.update(str);
            obj = JSON.parse(str);
        } catch (error) {
            this._log.error(`map:put - Import map can not be parsed`);
            this._log.trace(error);
            throw new HttpError.UnsupportedMediaType();
        }

        // Write file to storage.
        try {
            this._log.info(`map:put - Start writing import map to sink - Pathname: ${path}`);
            await writeJSON(this._sink, path, obj, 'application/json');
        } catch (error) {
            this._log.error(`map:put - Failed writing import map to sink - Pathname: ${path}`);
            this._log.trace(error);
            throw new HttpError.BadGateway();
        }

        this._log.info(`map:put - Successfully wrote import map to sink - Pathname: ${path}`);

        return `sha512-${hasher.digest('base64')}`;
    }

    async _readVersions (incoming) {
        const path = createFilePathToVersion(incoming);
        let versions;
        try {
            const obj = await readJSON(this._sink, path);
            versions = new Versions(obj);
            this._log.info(
                `map:put - Successfully read version meta file from sink - Pathname: ${path}`,
            );
        } catch (error) {
            // File does not exist, its probably a new package
            versions = new Versions(incoming);
            this._log.info(
                `map:put - Version meta file did not exist in sink - Create new - Pathname: ${path}`,
            );
        }
        return versions;
    }

    async _writeVersions(incoming, versions) {
        const path = createFilePathToVersion(incoming);
        await writeJSON(this._sink, path, versions, 'application/json');
        this._log.info(
            `map:put - Successfully wrote version meta file to sink - Pathname: ${path}`,
        );
    }

    async handler(req, user, name, version) {
        const end = this._histogram.timer();

        const pVersion = decodeUriComponent(version);
        const pName = decodeUriComponent(name);

        try {
            validators.version(pVersion);
            validators.name(pName);
        } catch (error) {
            this._log.info(`map:put - Validation failed - ${error.message}`);
            const e = new HttpError.BadRequest();
            end({ labels: { success: false, status: e.status } });
            throw e;
        }

        const url = originalUrl(req);
        const org = this._orgRegistry.get(url.hostname);

        if (!org) {
            this._log.info(`map:put - Hostname does not match a configured organization - ${url.hostname}`);
            const e = new HttpError.BadRequest();
            end({ labels: { success: false, status: e.status, type: 'map' } });
            throw e;
        }

        const author = new Author(user);

        const incoming = new HttpIncoming(req, {
            type: 'map',
            version: pVersion,
            author,
            name: pName,
            org,
        });

        const versions = await this._readVersions(incoming);

        if (!versions.check(pVersion)) {
            this._log.info(
                `map:put - Semver version is lower than previous version of the package - Org: ${org} - Name: ${pName} - Version: ${pVersion}`,
            );
            const e = new HttpError.Conflict();
            end({ labels: { success: false, status: e.status, type: 'map' } });
            throw e;
        }

        const integrity = await this._parser(incoming);
        versions.setVersion(pVersion, integrity);

        try {
            await this._writeVersions(incoming, versions);
        } catch (error) {
            const e = new HttpError.BadGateway();
            end({ labels: { success: false, status: e.status, type: 'map' } });
            throw e;
        }

        const outgoing = new HttpOutgoing();
        outgoing.cacheControl = this._cacheControl;
        outgoing.statusCode = 303;
        outgoing.location = createURIPathToImportMap(incoming);

        end({ labels: { status: outgoing.statusCode, type: 'map' } });

        return outgoing;
    }
};
export default MapPut;
