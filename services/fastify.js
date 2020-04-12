'use strict';

const { PassThrough } = require('stream');
const fastify = require('fastify');
const abslog = require('abslog');
const cors = require('fastify-cors');
const jwt = require('fastify-jwt');

const { http, sink, prop } = require('..');
const utils = require('./fastify-utils');

const FastifyService = class FastifyService {
    constructor({
        customSink,
        port = 4001,
        address = 'localhost',
        logger,
        config = {},
    } = {}) {
        this.sink = customSink || new sink.FS();
        this.log = abslog(logger);
        this.address = address;
        this.port = port;
        this.app = fastify({
            ignoreTrailingSlash: true,
            prefixTrailingSlash: 'no-slash',
            modifyCoreObjects: false,
            trustProxy: true,
            logger: false,
        });

        this.app.register(cors);

        // Authentication
        this.app.register(jwt, {
            secret: 'supersecret',
            messages: {
                badRequestErrorMessage: 'Autorization header is malformatted. Format is "Authorization: Bearer [token]"',
                noAuthorizationInHeaderMessage: 'Autorization header is missing!',
                authorizationTokenExpiredMessage: 'Authorization token expired',
                authorizationTokenInvalid: 'Authorization token is invalid'
            }
        });

        this.app.decorate('authenticate', async (request, reply) => {
            try {
              await request.jwtVerify()
            } catch (error) {
              reply.send(error)
            }
        });

        this.authOptions = {
            preValidation: [this.app.authenticate]
        }

        // Handle multipart upload
        const _multipart = Symbol('multipart');

        function setMultipart(req, done) {
            req[_multipart] = true;
            done();
        }
        this.app.addContentTypeParser('multipart', setMultipart);

        // Error handling
        this.app.setErrorHandler((error, request, reply) => {
            this.log.error(error);
            if (error.statusCode) {
                reply.code(error.statusCode).send(error.message);
                return;
            }
            reply.code(500).send('Internal server error');
        });

        this.routes();

        this._versionsGet = new http.VersionsGet(this.sink, config, logger);
        this._aliasPost = new http.AliasPost(this.sink, config, logger);
        this._aliasDel = new http.AliasDel(this.sink, config, logger);
        this._aliasGet = new http.AliasGet(this.sink, config, logger);
        this._aliasPut = new http.AliasPut(this.sink, config, logger);
        this._authPost = new http.AuthPost(config, logger);
        this._pkgLog = new http.PkgLog(this.sink, config, logger);
        this._pkgGet = new http.PkgGet(this.sink, config, logger);
        this._pkgPut = new http.PkgPut(this.sink, config, logger);
        this._mapGet = new http.MapGet(this.sink, config, logger);
        this._mapPut = new http.MapPut(this.sink, config, logger);

        const mergeStreams = (...streams) => {
            const str = new PassThrough({ objectMode: true });

            // Avoid hitting the max listeners limit when multiple
            // streams is piped into the same stream.
            str.on('pipe', () => {
                str.setMaxListeners(str.getMaxListeners() + 1);
            });

            str.on('unpipe', () => {
                str.setMaxListeners(str.getMaxListeners() - 1);
            });

            for (const stm of streams) {
                stm.on('error', err => {
                    this.log.error(err);
                });
                stm.pipe(str);
            }
            return str;
        };

        // pipe metrics
        this.metrics = mergeStreams(
            this._versionsGet.metrics,
            this._aliasPost.metrics,
            this._aliasDel.metrics,
            this._aliasGet.metrics,
            this._aliasPut.metrics,
            this._authPost.metrics,
            this._pkgLog.metrics,
            this._pkgGet.metrics,
            this._pkgPut.metrics,
            this._mapGet.metrics,
            this._mapPut.metrics,
        ).on('error', err => {
            this.log.error(err);
        });
    }

    routes() {
        //
        // Authentication
        //

        // curl -X POST -i -F key=foo http://localhost:4001/auth/login

        this.app.post(`/${prop.base_auth}/login`, async (request, reply) => {
            // const params = utils.sanitizeParameters(request.raw.url);
            const outgoing = await this._authPost.handler(
                request.req,
            );

            const token = this.app.jwt.sign(outgoing.body, {
                expiresIn: '7d',
            });

            // reply.header('etag', outgoing.etag);
            reply.type(outgoing.mimeType);
            reply.code(outgoing.statusCode);
            reply.send({ token });
        });

        //
        // Packages
        //

        // Get public package - scoped
        // curl -X GET http://localhost:4001/pkg/@cuz/fuzz/8.4.1/main/index.js
        this.app.get(`/${prop.base_pkg}/@:scope/:name/:version/*`, async (request, reply) => {
            const params = utils.sanitizeParametersX(request.raw.url);
            const outgoing = await this._pkgGet.handler(
                request.req,
                // params.org,
                params.name,
                params.version,
                params.extras,
            );
            reply.header('etag', outgoing.etag);
            reply.type(outgoing.mimeType);
            reply.code(outgoing.statusCode);
            reply.send(outgoing.stream);
        });

        // Get public package - non-scoped
        // curl -X GET http://localhost:4001/pkg/fuzz/8.4.1/main/index.js
        this.app.get(`/${prop.base_pkg}/:name/:version/*`, async (request, reply) => {
            const params = utils.sanitizeParametersX(request.raw.url);
            const outgoing = await this._pkgGet.handler(
                request.req,
                // params.org,
                params.name,
                params.version,
                params.extras,
            );
            reply.header('etag', outgoing.etag);
            reply.type(outgoing.mimeType);
            reply.code(outgoing.statusCode);
            reply.send(outgoing.stream);
        });

        // Get package overview - scoped
        // curl -X GET http://localhost:4001/pkg/@cuz/fuzz/8.4.1/
        this.app.get(
            `/${prop.base_pkg}/@:scope/:name/:version`,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._pkgLog.handler(
                    request.req,
                    // params.org,
                    params.name,
                    params.version,
                );
                reply.header('etag', outgoing.etag);
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.send(outgoing.stream);
            },
        );

        // Get package overview - non-scoped
        // curl -X GET http://localhost:4001/pkg/fuzz/8.4.1/
        this.app.get(
            `/${prop.base_pkg}/:name/:version`,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._pkgLog.handler(
                    request.req,
                    // params.org,
                    params.name,
                    params.version,
                );
                reply.header('etag', outgoing.etag);
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.send(outgoing.stream);
            },
        );

        // Get package versions - scoped
        // curl -X GET http://localhost:4001/pkg/@cuz/fuzz/
        this.app.get(`/${prop.base_pkg}/@:scope/:name`, async (request, reply) => {
            const params = utils.sanitizeParametersX(request.raw.url);
            const outgoing = await this._versionsGet.handler(
                request.req,
                // params.org,
                prop.base_pkg,
                params.name,
            );
            reply.header('etag', outgoing.etag);
            reply.type(outgoing.mimeType);
            reply.code(outgoing.statusCode);
            reply.send(outgoing.stream);
        });

        // Get package versions - non-scoped
        // curl -X GET http://localhost:4001/pkg/fuzz/
        this.app.get(`/${prop.base_pkg}/:name`, async (request, reply) => {
            const params = utils.sanitizeParametersX(request.raw.url);
            const outgoing = await this._versionsGet.handler(
                request.req,
                // params.org,
                prop.base_pkg,
                params.name,
            );
            reply.header('etag', outgoing.etag);
            reply.type(outgoing.mimeType);
            reply.code(outgoing.statusCode);
            reply.send(outgoing.stream);
        });

        // Put package - scoped
        // curl -X PUT -i -F filedata=@archive.tgz http://localhost:4001/pkg/@cuz/fuzz/8.4.1/
        this.app.put(
            `/${prop.base_pkg}/@:scope/:name/:version`,
            this.authOptions,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._pkgPut.handler(
                    request.req,
//                    params.org,
                    params.name,
                    params.version,
                );
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.redirect(outgoing.location);
            },
        );

        // Put package - non-scoped
        // curl -X PUT -i -F filedata=@archive.tgz http://localhost:4001/pkg/fuzz/8.4.1/
        this.app.put(
            `/${prop.base_pkg}/:name/:version`,
            this.authOptions,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._pkgPut.handler(
                    request.req,
//                    params.org,
                    params.name,
                    params.version,
                );
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.redirect(outgoing.location);
            },
        );


        //
        // Import Maps
        //

        // Get map - scoped
        // curl -X GET http://localhost:4001/map/@cuz/buzz/4.2.2
        this.app.get(
            `/${prop.base_map}/@:scope/:name/:version`,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._mapGet.handler(
                    request.req,
                    // params.org,
                    params.name,
                    params.version,
                );
                reply.header('etag', outgoing.etag);
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.send(outgoing.stream);
            },
        );

        // Get map - non-scoped
        // curl -X GET http://localhost:4001/map/buzz/4.2.2
        this.app.get(
            `/${prop.base_map}/:name/:version`,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._mapGet.handler(
                    request.req,
                    // params.org,
                    params.name,
                    params.version,
                );
                reply.header('etag', outgoing.etag);
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.send(outgoing.stream);
            },
        );

        // Get map versions - scoped
        // curl -X GET http://localhost:4001/map/@cuz/buzz
        this.app.get(`/${prop.base_map}/@:scope/:name`, async (request, reply) => {
            const params = utils.sanitizeParametersX(request.raw.url);
            const outgoing = await this._versionsGet.handler(
                request.req,
                // params.org,
                prop.base_map,
                params.name,
            );
            reply.header('etag', outgoing.etag);
            reply.type(outgoing.mimeType);
            reply.code(outgoing.statusCode);
            reply.send(outgoing.stream);
        });

        // Get map versions - non-scoped
        // curl -X GET http://localhost:4001/map/buzz
        this.app.get(`/${prop.base_map}/:name`, async (request, reply) => {
            const params = utils.sanitizeParametersX(request.raw.url);
            const outgoing = await this._versionsGet.handler(
                request.req,
                // params.org,
                prop.base_map,
                params.name,
            );
            reply.header('etag', outgoing.etag);
            reply.type(outgoing.mimeType);
            reply.code(outgoing.statusCode);
            reply.send(outgoing.stream);
        });

        // Put map - scoped
        // curl -X PUT -i -F map=@import-map.json http://localhost:4001/map/@cuz/buzz/4.2.2
        this.app.put(
            `/${prop.base_map}/@:scope/:name/:version`,
            this.authOptions,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._mapPut.handler(
                    request.req,
                    // params.org,
                    params.name,
                    params.version,
                );
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.redirect(outgoing.location);
            },
        );

        // Put map - non-scoped
        // curl -X PUT -i -F map=@import-map.json http://localhost:4001/map/buzz/4.2.2
        this.app.put(
            `/${prop.base_map}/:name/:version`,
            this.authOptions,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._mapPut.handler(
                    request.req,
                    // params.org,
                    params.name,
                    params.version,
                );
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.redirect(outgoing.location);
            },
        );

        //
        // Alias Packages
        //

        // curl -X GET -L http://localhost:4001/pkg/@cuz/fuzz/v8

        this.app.get(
            `/${prop.base_pkg}/@:scope/:name/v:alias`,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._aliasGet.handler(
                    request.req,
                    // params.org,
                    prop.base_pkg,
                    params.name,
                    params.alias,
                );
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.redirect(outgoing.location);
            },
        );

        // curl -X GET -L http://localhost:4001/pkg/fuzz/v8

        this.app.get(
            `/${prop.base_pkg}/:name/v:alias`,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._aliasGet.handler(
                    request.req,
                    // params.org,
                    prop.base_pkg,
                    params.name,
                    params.alias,
                );
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.redirect(outgoing.location);
            },
        );

        // curl -X GET -L http://localhost:4001/pkg/@cuz/fuzz/v8/main/index.js

        this.app.get(
            `/${prop.base_pkg}/@:scope/:name/v:alias/*`,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._aliasGet.handler(
                    request.req,
                    // params.org,
                    prop.base_pkg,
                    params.name,
                    params.alias,
                    params.extras,
                );
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.redirect(outgoing.location);
            },
        );

        // curl -X GET -L http://localhost:4001/pkg/fuzz/v8/main/index.js

        this.app.get(
            `/${prop.base_pkg}/:name/v:alias/*`,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._aliasGet.handler(
                    request.req,
                    // params.org,
                    prop.base_pkg,
                    params.name,
                    params.alias,
                    params.extras,
                );
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.redirect(outgoing.location);
            },
        );

        // curl -X PUT -i -F version=8.4.1 http://localhost:4001/pkg/@cuz/fuzz/v8

        this.app.put(
            `/${prop.base_pkg}/@:scope/:name/v:alias`,
            this.authOptions,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._aliasPut.handler(
                    request.req,
                    // params.org,
                    prop.base_pkg,
                    params.name,
                    params.alias,
                );
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.redirect(outgoing.location);
            },
        );

        // curl -X PUT -i -F version=8.4.1 http://localhost:4001/pkg/fuzz/v8

        this.app.put(
            `/${prop.base_pkg}/:name/v:alias`,
            this.authOptions,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._aliasPut.handler(
                    request.req,
                    // params.org,
                    prop.base_pkg,
                    params.name,
                    params.alias,
                );
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.redirect(outgoing.location);
            },
        );

        // curl -X POST -i -F version=8.4.1 http://localhost:4001/pkg/@cuz/lit-html/v8

        this.app.post(
            `/${prop.base_pkg}/@:scope/:name/v:alias`,
            this.authOptions,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._aliasPost.handler(
                    request.req,
                    // params.org,
                    prop.base_pkg,
                    params.name,
                    params.alias,
                );
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.redirect(outgoing.location);
            },
        );

        // curl -X POST -i -F version=8.4.1 http://localhost:4001/pkg/lit-html/v8

        this.app.post(
            `/${prop.base_pkg}/:name/v:alias`,
            this.authOptions,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._aliasPost.handler(
                    request.req,
                    // params.org,
                    prop.base_pkg,
                    params.name,
                    params.alias,
                );
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.redirect(outgoing.location);
            },
        );

        // curl -X DELETE http://localhost:4001/pkg/@cuz/fuzz/v8

        this.app.delete(
            `/${prop.base_pkg}/@:scope/:name/v:alias`,
            this.authOptions,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._aliasDel.handler(
                    request.req,
                    // params.org,
                    prop.base_pkg,
                    params.name,
                    params.alias,
                );
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.send(outgoing.body);
            },
        );

        // curl -X DELETE http://localhost:4001/pkg/fuzz/v8

        this.app.delete(
            `/${prop.base_pkg}/:name/v:alias`,
            this.authOptions,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._aliasDel.handler(
                    request.req,
                    // params.org,
                    prop.base_pkg,
                    params.name,
                    params.alias,
                );
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.send(outgoing.body);
            },
        );


        //
        // Alias Import Maps
        //

        // curl -X GET -L http://localhost:4001/map/@cuz/buzz/v4

        this.app.get(
            `/${prop.base_map}/@:scope/:name/v:alias`,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._aliasGet.handler(
                    request.req,
                    // params.org,
                    prop.base_map,
                    params.name,
                    params.alias,
                );
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.redirect(outgoing.location);
            },
        );

        // curl -X GET -L http://localhost:4001/map/buzz/v4

        this.app.get(
            `/${prop.base_map}/:name/v:alias`,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._aliasGet.handler(
                    request.req,
                    // params.org,
                    prop.base_map,
                    params.name,
                    params.alias,
                );
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.redirect(outgoing.location);
            },
        );

        // curl -X PUT -i -F version=4.2.2 http://localhost:4001/map/@cuz/buzz/v4

        this.app.put(
            `/${prop.base_map}/@:scope/:name/v:alias`,
            this.authOptions,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._aliasPut.handler(
                    request.req,
                    // params.org,
                    prop.base_map,
                    params.name,
                    params.alias,
                );
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.redirect(outgoing.location);
            },
        );

        // curl -X PUT -i -F version=4.2.2 http://localhost:4001/map/buzz/v4

        this.app.put(
            `/${prop.base_map}/:name/v:alias`,
            this.authOptions,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._aliasPut.handler(
                    request.req,
                    // params.org,
                    prop.base_map,
                    params.name,
                    params.alias,
                );
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.redirect(outgoing.location);
            },
        );

        // curl -X POST -i -F version=4.4.2 http://localhost:4001/map/@cuz/buzz/v4

        this.app.post(
            `/${prop.base_map}/@:scope/:name/v:alias`,
            this.authOptions,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._aliasPost.handler(
                    request.req,
                    // params.org,
                    prop.base_map,
                    params.name,
                    params.alias,
                );
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.redirect(outgoing.location);
            },
        );

        // curl -X POST -i -F version=4.4.2 http://localhost:4001/map/buzz/v4

        this.app.post(
            `/${prop.base_map}/:name/v:alias`,
            this.authOptions,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._aliasPost.handler(
                    request.req,
                    // params.org,
                    prop.base_map,
                    params.name,
                    params.alias,
                );
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.redirect(outgoing.location);
            },
        );

        // curl -X DELETE http://localhost:4001/map/@cuz/buzz/v4

        this.app.delete(
            `/${prop.base_map}/@:scope/:name/v:alias`,
            this.authOptions,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._aliasDel.handler(
                    request.req,
                    // params.org,
                    prop.base_map,
                    params.name,
                    params.alias,
                );
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.send(outgoing.body);
            },
        );

        // curl -X DELETE http://localhost:4001/map/buzz/v4

        this.app.delete(
            `/${prop.base_map}/:name/v:alias`,
            this.authOptions,
            async (request, reply) => {
                const params = utils.sanitizeParametersX(request.raw.url);
                const outgoing = await this._aliasDel.handler(
                    request.req,
                    // params.org,
                    prop.base_map,
                    params.name,
                    params.alias,
                );
                reply.type(outgoing.mimeType);
                reply.code(outgoing.statusCode);
                reply.send(outgoing.body);
            },
        );
    }

    async start() {
        try {
            const address = await this.app.listen(this.port, this.address);
            return address;
        } catch (err) {
            this.app.log.error(err);
            throw err;
        }
    }

    async stop() {
        try {
            await this.app.close();
        } catch (err) {
            this.app.log.error(err);
            throw err;
        }
    }
}

module.exports = FastifyService;

if (require.main === module) {
    const service = new FastifyService();
    service.start().catch(() => {
        process.exit(1);
    });
}
