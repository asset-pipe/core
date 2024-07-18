import validators from '@eik/common-validators';
import originalUrl from 'original-url';
import HttpError from 'http-errors';
import Metrics from '@metrics/client';
import abslog from 'abslog';

import { createFilePathToVersion } from '../utils/path-builders-fs.js';
import { decodeUriComponent } from '../utils/utils.js';
import HttpOutgoing from '../classes/http-outgoing.js';
import config from '../utils/defaults.js';

const VersionsGet = class VersionsGet {
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
            name: 'eik_core_versions_get_handler',
            description:
                'Histogram measuring time taken in @eik/core VersionsGet handler method',
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

    async handler(req, type, name) {
        const end = this._histogram.timer();

        const pName = decodeUriComponent(name);

        try {
            validators.name(pName);
            validators.type(type);
        } catch (error) {
            this._log.debug(
                `pkg:latest - Validation failed - ${error.message}`,
            );
            const e = new HttpError.NotFound();
            end({ labels: { success: false, status: e.status } });
            throw e;
        }

        const url = originalUrl(req);
        const org = this._orgRegistry.get(url.hostname);

        if (!org) {
            this._log.info(`pkg:latest - Hostname does not match a configured organization - ${url.hostname}`);
            const e = new HttpError.BadRequest();
            end({ labels: { success: false, status: e.status, type } });
            throw e;
        }
        const path = createFilePathToVersion({ org, type, name: pName });

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

            this._log.debug(
                `pkg:latest - Package log found - Pathname: ${path}`,
            );

            end({ labels: { status: outgoing.statusCode, type } });

            return outgoing;
        } catch (error) {
            this._log.debug(
                `pkg:latest - Package log found - Pathname: ${path}`,
            );
            const e = new HttpError.NotFound();
            end({ labels: { success: false, status: e.status, type } });
            throw e;
        }
    }
};
export default VersionsGet;
