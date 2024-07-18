import validators from '@eik/common-validators';
import originalUrl from 'original-url';
import HttpError from 'http-errors';
import Metrics from '@metrics/client';
import abslog from 'abslog';

import {
    createFilePathToPackage,
    createFilePathToVersion,
    createFilePathToEikJson
} from '../utils/path-builders-fs.js';
import { decodeUriComponent, writeJSON, readJSON, readEikJson } from '../utils/utils.js';
import { createURIPathToPkgLog } from '../utils/path-builders-uri.js';
import MultipartParser from '../multipart/parser.js';
import HttpIncoming from '../classes/http-incoming.js';
import HttpOutgoing from '../classes/http-outgoing.js';
import Versions from '../classes/versions.js';
import Package from '../classes/package.js';
import Author from '../classes/author.js';
import config from '../utils/defaults.js';

const PkgPut = class PkgPut {
    constructor({
        pkgMaxFileSize,
        organizations,
        cacheControl,
        logger,
        sink,
    } = {}) {
        this._pkgMaxFileSize = pkgMaxFileSize || config.pkgMaxFileSize;
        this._organizations = organizations || config.organizations;
        this._cacheControl = cacheControl;
        this._sink = sink;
        this._log = abslog(logger);
        this._metrics = new Metrics();
        this._histogram = this._metrics.histogram({
            name: 'eik_core_pkg_put_handler',
            description:
                'Histogram measuring time taken in @eik/core PkgPut handler method',
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
            legalFiles: ['package'],
            sink: this._sink,
        });
    }

    get metrics() {
        return this._metrics;
    }

    async _parser(incoming) {
        return new Promise((resolve, reject) => {
            this._multipart.parse(incoming).then((result) => {
                const pkg = new Package(incoming);
                result.forEach(obj => {
                    if (obj.constructor.name === 'FormField') {
                        pkg.setMeta(obj);
                    }
                    if (obj.constructor.name === 'FormFile') {
                        obj.value.forEach((o) => {
                            pkg.setAsset(o);
                        })
                    }
                });
                return pkg;
            }).then(async (pkg) => {
                const path = createFilePathToPackage(pkg);
                await writeJSON(
                    this._sink,
                    path,
                    pkg,
                    'application/json',
                );

                this._log.info(
                    `pkg:put - Successfully wrote package meta file to sink - Pathname: ${path}`,
                );

                resolve(pkg);
            }).catch((error) => {
                reject(error);
            });
        });
    }

    async _readVersions(incoming) {
        const path = createFilePathToVersion(incoming);
        let versions;
        try {
            const obj = await readJSON(this._sink, path);
            versions = new Versions(obj);
            this._log.info(
                `pkg:put - Successfully read version meta file from sink - Pathname: ${path}`,
            );
        } catch (error) {
            // File does not exist, its probably a new package
            versions = new Versions(incoming);
            this._log.info(
                `pkg:put - Version meta file did not exist in sink - Create new - Pathname: ${path}`,
            );
        }
        return versions;
    }

    async _readVersion(incoming) {
        const path = createFilePathToEikJson(incoming);
        try {
            await readEikJson(this._sink, path);
            this._log.info(
                `pkg:put - Found version meta file from sink - Pathname: ${path}`,
            );
            return true;
        } catch (error) {

            // File does not exist, its probably a new package
            this._log.info(
                `pkg:put - Did not find meta file in sink - Create new - Pathname: ${path}`,
            );
            return false;
        }
    }

    async _writeVersions(incoming, versions) {
        const path = createFilePathToVersion(incoming);
        await writeJSON(this._sink, path, versions, 'application/json');
        this._log.info(
            `pkg:put - Successfully wrote version meta file to sink - Pathname: ${path}`,
        );
    }

    async handler(req, user, type, name, version) {

        const end = this._histogram.timer();

        const pVersion = decodeUriComponent(version);
        const pName = decodeUriComponent(name);

        try {
            validators.version(pVersion);
            validators.name(pName);
            validators.type(type);
        } catch (error) {
            this._log.info(`pkg:put - Validation failed - ${error.message}`);
            const e = new HttpError.BadRequest();
            end({ labels: { success: false, status: e.status } });
            throw e;
        }

        const url = originalUrl(req);
        const org = this._orgRegistry.get(url.hostname);

        if (!org) {
            this._log.info(`pkg:put - Hostname does not match a configured organization - ${url.hostname}`);
            const e = new HttpError.BadRequest();
            end({ labels: { success: false, status: e.status, type } });
            throw e;
        }

        const author = new Author(user);

        const incoming = new HttpIncoming(req, {
            version: pVersion,
            author,
            type,
            name: pName,
            org,
        });

        const versionExists = await this._readVersion(incoming);

        if (versionExists) {
            this._log.info(
                `pkg:put - Semver version already exists for the package - Org: ${org} - Name: ${pName} - Version: ${pVersion}`,
            );
            const e = new HttpError.Conflict();
            end({ labels: { success: false, status: e.status, type } });
            throw e;
        }

        const versions = await this._readVersions(incoming);
        const pkg = await this._parser(incoming);
        versions.setVersion(pVersion, pkg.integrity);

        try {
            await this._writeVersions(incoming, versions);
        } catch (error) {
            const e = new HttpError.BadGateway();
            end({ labels: { success: false, status: e.status, type } });
            throw e;
        }

        const outgoing = new HttpOutgoing();
        outgoing.cacheControl = this._cacheControl;
        outgoing.statusCode = 303;
        outgoing.location = createURIPathToPkgLog(pkg);

        end({ labels: { status: outgoing.statusCode, type } });

        return outgoing;
    }
};
export default PkgPut;
