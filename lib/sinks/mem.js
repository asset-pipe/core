import { Writable, Readable } from 'node:stream';
import { ReadFile } from '@eik/common-utils';
import { join } from 'node:path';
import Metrics from '@metrics/client';
import Sink from '@eik/sink';

import Entry from './mem-entry.js';

const DEFAULT_ROOT_PATH = '/eik';

/**
 * A sink for persisting files to memory
 *
 * @class SinkMem
 */

const SinkMem = class SinkMem extends Sink {
    constructor({ rootPath = DEFAULT_ROOT_PATH } = {}) {
        super();
        this._rootPath = rootPath;
        this._metrics = new Metrics();
        this._counter = this._metrics.counter({
            name: 'eik_core_sink_mem',
            description: 'Counter measuring access to the in memory storage sink',
            labels: {
                operation: 'n/a',
                success: false,
                access: false,
            },
        });
        this._state = new Map();
    }

    get metrics() {
        return this._metrics;
    }

    write(filePath, contentType) {
        return new Promise((resolve, reject) => {
            const operation = 'write';

            try {
                super.constructor.validateFilePath(filePath);
                super.constructor.validateContentType(contentType);
            } catch (error) {
                this._counter.inc({ labels: { operation } });
                reject(error);
                return;
            }

            const pathname = join(this._rootPath, filePath);

            if (pathname.indexOf(this._rootPath) !== 0) {
                reject(new Error(`Directory traversal - ${filePath}`));
                this._counter.inc({ labels: { operation } });
                return;
            }

            const payload = [];
            const stream = new Writable({
                write(chunk, encoding, cb) {
                    payload.push(chunk);
                    cb();
                },
            });

            stream.on('finish', () => {
                const entry = new Entry({
                    mimeType: contentType,
                    payload
                });
                
                this._state.set(pathname, entry);
                
                this._counter.inc({ labels: { 
                    success: true, 
                    access: true, 
                    operation 
                } });
            });

            resolve(stream);
        });
    }

    read(filePath) {
        return new Promise((resolve, reject) => {
            const operation = 'read';

            try {
                super.constructor.validateFilePath(filePath);
            } catch (error) {
                this._counter.inc({ labels: { operation } });
                reject(error);
                return;
            }

            const pathname = join(this._rootPath, filePath);

            if (pathname.indexOf(this._rootPath) !== 0) {
                this._counter.inc({ labels: { operation } });
                reject(new Error(`Directory traversal - ${filePath}`));
                return;
            }

            const entry = this._state.get(pathname);
            const payload = entry.payload || [];
            const obj = new ReadFile( {
                mimeType: entry.mimeType,
                etag: entry.hash,
            });

            obj.stream = new Readable({
                read() {
                    payload.forEach(item => {
                        this.push(item);
                    });
                    this.push(null);
                },
            });

            obj.stream.on('end', () => {
                this._counter.inc({ labels: { 
                    success: true, 
                    access: true, 
                    operation 
                } });
            });

            resolve(obj);
        });
    }

    delete(filePath) {
        return new Promise((resolve, reject) => {
            const operation = 'delete';

            try {
                super.constructor.validateFilePath(filePath);
            } catch (error) {
                this._counter.inc({ labels: { operation } });
                reject(error);
                return;
            }

            const pathname = join(this._rootPath, filePath);

            if (pathname.indexOf(this._rootPath) !== 0) {
                this._counter.inc({ labels: { operation } });
                reject(new Error(`Directory traversal - ${filePath}`));
                return;
            }

            // Delete recursively
            Array.from(this._state.keys()).forEach((key) => {
                if (key.startsWith(pathname)) {
                    this._state.delete(key);
                }
            });

            this._counter.inc({ labels: { 
                success: true, 
                access: true, 
                operation 
            } });

            resolve();
        });
    }

    exist(filePath) {
        return new Promise((resolve, reject) => {
            const operation = 'exist';

            try {
                super.constructor.validateFilePath(filePath);
            } catch (error) {
                this._counter.inc({ labels: { operation } });
                reject(error);
                return;
            }

            const pathname = join(this._rootPath, filePath);

            if (pathname.indexOf(this._rootPath) !== 0) {
                this._counter.inc({ labels: { operation } });
                reject(new Error(`Directory traversal - ${filePath}`));
                return;
            }

            this._counter.inc({ labels: { 
                success: true, 
                access: true, 
                operation 
            } });

            if (this._state.has(pathname)) {
                resolve();
                return;
            }

            reject(new Error('File does not exist'));
        });
    }

    get [Symbol.toStringTag]() {
        return 'SinkMEM';
    }
}
export default SinkMem;
