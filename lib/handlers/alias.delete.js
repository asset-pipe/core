import validators from '@eik/common-validators';
import originalUrl from 'original-url';
import HttpError from 'http-errors';
import Metrics from '@metrics/client';
import abslog from 'abslog';

import { createFilePathToAlias } from '../utils/path-builders-fs.js';
import { decodeUriComponent } from '../utils/utils.js';
import HttpOutgoing from '../classes/http-outgoing.js';
import config from '../utils/defaults.js';

const AliasDel = class AliasDel {
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
            name: 'eik_core_alias_del_handler',
            description:
                'Histogram measuring time taken in @eik/core AliasDel handler method',
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

    async _exist(path = '') {
        try {
            await this._sink.exist(path);
            return true;
        } catch (error) {
            return false;
        }
    }

    async handler(req, user, type, name, alias) {
        const end = this._histogram.timer();

        const pAlias = decodeUriComponent(alias);
        const pName = decodeUriComponent(name);

        try {
            validators.alias(pAlias);
            validators.name(pName);
            validators.type(type);
        } catch (error) {
            this._log.info(`alias:del - Validation failed - ${error.message}`);
            const e = new HttpError.NotFound();
            end({ labels: { success: false, status: e.status } });
            throw e;
        }

        const url = originalUrl(req);
        const org = this._orgRegistry.get(url.hostname);

        if (!org) {
            this._log.info(`alias:del - Hostname does not match a configured organization - ${url.hostname}`);
            const e = new HttpError.BadRequest();
            end({ labels: { success: false, status: e.status, type } });
            throw e;
        }

        const path = createFilePathToAlias({ org, type, name: pName, alias: pAlias });
        const exist = await this._exist(path);
        if (!exist) {
            this._log.info(
                `alias:del - Alias does not exists - Org: ${org} - Type: ${type} - Name: ${pName} - Alias: ${pAlias}`,
            );
            const e = new HttpError.NotFound();
            end({ labels: { success: false, status: e.status, type } });
            throw e;
        }

        try {
            this._log.info(
                `alias:del - Start deleting alias from sink - Pathname: ${path}`,
            );
            await this._sink.delete(path);
        } catch (error) {
            this._log.error(
                `alias:del - Failed deleting alias from sink - Pathname: ${path}`,
            );
            this._log.trace(error);
            const e = new HttpError.BadGateway();
            end({ labels: { success: false, status: e.status, type } });
            return e;
        }

        this._log.info(
            `alias:del - Successfully deleted alias from sink - Pathname: ${path}`,
        );

        const outgoing = new HttpOutgoing();
        outgoing.cacheControl = this._cacheControl;
        outgoing.statusCode = 204;

        end({ labels: { status: outgoing.statusCode, type } });

        return outgoing;
    }
};
export default AliasDel;
