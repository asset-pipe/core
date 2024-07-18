import { ReadFile } from '@eik/common-utils';
import { rimraf } from 'rimraf';
import Metrics from '@metrics/client';
import Sink from '@eik/sink';
import mime from 'mime';
import path from 'node:path';
import fs from 'node:fs';

import { etagFromFsStat } from '../utils/utils.js';
import conf from '../utils/defaults.js';

/**
 * A sink for persisting files to local file system
 *
 * @class SinkFS
 */

const SinkFS = class SinkFS extends Sink {
    constructor(config = {}) {
        super();
        this._config = { ...conf, ...config};
        this._metrics = new Metrics();
        this._counter = this._metrics.counter({
            name: 'eik_core_sink_fs',
            description: 'Counter measuring access to the file system storage sink',
            labels: {
                operation: 'n/a',
                success: false,
                access: false,
            },
        });
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

            const pathname = path.join(this._config.sinkFsRootPath, filePath);

            if (pathname.indexOf(this._config.sinkFsRootPath) !== 0) {
                this._counter.inc({ labels: { operation } });
                reject(new Error(`Directory traversal - ${filePath}`));
                return;
            }

            const dir = path.dirname(pathname);

            fs.mkdir(
                dir,
                {
                    recursive: true,
                },
                error => {
                    if (error) {
                        this._counter.inc({ labels: { access: true, operation } });
                        reject(
                            new Error(`Could not create directory - ${dir}`),
                        );
                        return;
                    }

                    const stream = fs.createWriteStream(pathname, {
                        autoClose: true,
                        emitClose: true,
                    });

                    this._counter.inc({ labels: { 
                        success: true, 
                        access: true, 
                        operation 
                    } });

                    resolve(stream);
                },
            );
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

            const pathname = path.join(this._config.sinkFsRootPath, filePath);

            if (pathname.indexOf(this._config.sinkFsRootPath) !== 0) {
                this._counter.inc({ labels: { operation } });
                reject(new Error(`Directory traversal - ${filePath}`));
                return;
            }

            const closeFd = fd => {
                fs.close(fd, (error) => {
                    if (error) {
                        this._counter.inc({ labels: { 
                            access: true, 
                            operation 
                        } });
                        return;
                    }
                    this._counter.inc({ labels: { 
                        success: true,
                        access: true, 
                        operation 
                    } });
                });
            }

            fs.open(pathname, 'r', (error, fd) => {
                if (error) {
                    this._counter.inc({ labels: { 
                        access: true, 
                        operation 
                    } });
                    reject(error);
                    return;
                };

                fs.fstat(fd, (err, stat) => {
                    if (err) {
                        closeFd(fd);
                        reject(err);
                        return;
                    };

                    if (!stat.isFile()) {
                        closeFd(fd);
                        reject(new Error(`Not a file - ${pathname}`));
                        return;
                    }

                    const mimeType = mime.getType(pathname) || 'application/octet-stream';
                    const etag = etagFromFsStat(stat);

                    const obj = new ReadFile({
                        mimeType,
                        etag,
                    });

                    obj.stream = fs.createReadStream(pathname, {
                        autoClose: true,
                        fd,
                    });

                    obj.stream.on('error', () => {
                        this._counter.inc({ labels: { 
                            access: true, 
                            operation 
                        } });
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

            });
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

            const pathname = path.join(this._config.sinkFsRootPath, filePath);

            if (pathname.indexOf(this._config.sinkFsRootPath) !== 0) {
                this._counter.inc({ labels: { operation } });
                reject(new Error(`Directory traversal - ${filePath}`));
                return;
            }

            rimraf(pathname).then(() => {
                this._counter.inc({ labels: { 
                    success: true, 
                    access: true, 
                    operation 
                } });
                resolve();
            }).catch((error) => {
                this._counter.inc({ labels: { access: true, operation } });
                reject(error);
                
            });
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

            const pathname = path.join(this._config.sinkFsRootPath, filePath);

            if (pathname.indexOf(this._config.sinkFsRootPath) !== 0) {
                this._counter.inc({ labels: { operation } });
                reject(new Error(`Directory traversal - ${filePath}`));
                return;
            }

            fs.stat(pathname, (error, stat) => {
                this._counter.inc({ labels: { success: true, access: true, operation } });
                
                if (stat && stat.isFile()) {
                    resolve();
                    return;
                }

                if (error) {
                    reject(error);
                    return;
                }
                reject();
            });
        });
    }

    get [Symbol.toStringTag]() {
        return 'SinkFS';
    }
}
export default SinkFS;
