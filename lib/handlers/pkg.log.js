import validators from '@eik/common-validators';
import originalUrl from 'original-url';
import HttpError from 'http-errors';
import abslog from 'abslog';
import Metrics from '@metrics/client';

import { createFilePathToPackage } from '../utils/path-builders-fs.js';
import { decodeUriComponent } from '../utils/utils.js';
import HttpOutgoing from '../classes/http-outgoing.js';
import config from '../utils/defaults.js';

const PkgLog = class PkgLog {
    constructor({
        organizations,
        cacheControl,
        logger,
        sink,
        etag,
    } = {}) {
        this._organizations = organizations || config.organizations;
        this._cacheControl = cacheControl || 'no-cache';
        this._sink = sink;
        this._etag = etag || config.etag;
        this._log = abslog(logger);
        this._metrics = new Metrics();
        this._histogram = this._metrics.histogram({
            name: 'eik_core_pkg_log_handler',
            description:
                'Histogram measuring time taken in @eik/core PkgLog handler method',
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
    }

    get metrics() {
        return this._metrics;
    }

    async handler(req, type, name, version) {
        const end = this._histogram.timer();

        const pVersion = decodeUriComponent(version);
        const pName = decodeUriComponent(name);

        try {
            validators.version(pVersion);
            validators.name(pName);
            validators.type(type);
        } catch (error) {
            this._log.debug(`pkg:log - Validation failed - ${error.message}`);
            const e = new HttpError.NotFound();
            end({ labels: { success: false, status: e.status, type } });
            throw e;
        }

        const url = originalUrl(req);
        const org = this._orgRegistry.get(url.hostname);

        if (!org) {
            this._log.info(`pkg:log - Hostname does not match a configured organization - ${url.hostname}`);
            const e = new HttpError.BadRequest();
            end({ labels: { success: false, status: e.status, type } });
            throw e;
        }

        const path = createFilePathToPackage({ org, type, name: pName, version: pVersion });

        try {
            const file = await this._sink.read(path);
            const outgoing = new HttpOutgoing();
            outgoing.cacheControl = this._cacheControl;
            outgoing.mimeType = 'application/json';

            if (this._etag) {
                outgoing.etag = file.etag;
            }

            if (
                this._etag &&
                req.headers['if-none-match'] === file.etag
            ) {
                outgoing.statusCode = 304;
                file.stream.destroy();
            } else {
                outgoing.statusCode = 200;
                outgoing.stream = file.stream;
            }

            this._log.debug(`pkg:log - Package log found - Pathname: ${path}`);

            end({ labels: { status: outgoing.statusCode, type } });

            return outgoing;
        } catch (error) {
            this._log.debug(`pkg:log - Package log found - Pathname: ${path}`);
            const e = new HttpError.NotFound();
            end({ labels: { success: false, status: e.status, type } });
            throw e;
        }
    }
};
export default PkgLog;
