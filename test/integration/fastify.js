'use strict';

const { test } = require('tap');
const fetch = require('node-fetch');
const FormData = require('form-data');
const { join } = require('path');
const { createReadStream } = require('fs');
const FastifyService = require('../../services/fastify');
const SinkTest = require('../../fixtures/sink-test');

test('Packages GET', async t => {
    const sink = new SinkTest();
    const service = new FastifyService({ customSink: sink, port: 0, logger: false });
    const address = await service.start();

    sink.set('/biz/pkg/fuzz/8.4.1/main/index.js', 'hello world');

    const res = await fetch(
        `${address}/biz/pkg/fuzz/8.4.1/main/index.js`,
    );
    const body = await res.text();
    t.equals(res.status, 200, 'server should respond with 200 ok');
    t.equals(body, 'hello world', 'server should respond with file contents');

    await service.stop();
});

test('Packages PUT - all files extracted, files accessible after upload', async t => {
    const sink = new SinkTest();
    const service = new FastifyService({ customSink: sink, logger: false, port: 0 });
    const address = await service.start();

    const formData = new FormData();
    formData.append(
        'filedata',
        createReadStream(join(__dirname, '../../fixtures/archive.tgz')),
    );

    const res = await fetch(`${address}/foo/pkg/bar/1.1.1`, {
        method: 'PUT',
        body: formData,
        headers: formData.getHeaders(),
    });

    t.equals(res.status, 201, 'server PUT should respond with 201 ok');

    const file1 = await fetch(
        `${address}/foo/pkg/bar/1.1.1/main/index.js`,
    );
    const file2 = await fetch(
        `${address}/foo/pkg/bar/1.1.1/main/index.js.map`
    );
    const file3 = await fetch(
        `${address}/foo/pkg/bar/1.1.1/ie11/index.js`,
    );
    const file4 = await fetch(
        `${address}/foo/pkg/bar/1.1.1/ie11/index.js.map`,
    );
    const file5 = await fetch(
        `${address}/foo/pkg/bar/1.1.1/main/index.css`,
    );
    const file6 = await fetch(
        `${address}/foo/pkg/bar/1.1.1/main/index.css.map`,
    );
    const file7 = await fetch(
        `${address}/foo/pkg/bar/1.1.1/assets.json`,
    );

    t.equals(file1.status, 200, 'GET to index.js responded with 200 ok');
    t.equals(file2.status, 200, 'GET to index.js.map responded with 200 ok');
    t.equals(file3.status, 200, 'GET to ie11 index.js responded with 200 ok');
    t.equals(
        file4.status,
        200,
        'GET to ie11 index.js.map responded with 200 ok',
    );
    t.equals(file5.status, 200, 'GET to index.css responded with 200 ok');
    t.equals(file6.status, 200, 'GET to index.css.map responded with 200 ok');
    t.equals(file7.status, 200, 'GET to assets.json responded with 200 ok');

    await service.stop();
});

test('Packages PUT - all files extracted, correct response received', async t => {
    const sink = new SinkTest();
    const service = new FastifyService({ customSink: sink, port: 0, logger: false });
    const address = await service.start();

    const formData = new FormData();
    formData.append(
        'filedata',
        createReadStream(join(__dirname, '../../fixtures/archive.tgz')),
    );

    const res = await fetch(`${address}/biz/pkg/frazz/2.1.4`, {
        method: 'PUT',
        body: formData,
        headers: formData.getHeaders(),
    });
    const obj = await res.json();

    t.equals(res.status, 201, 'server should respond with 201 ok');

    t.equal(
        obj.files[0].pathname,
        '/biz/pkg/frazz/2.1.4/main/index.js',
        'JavaScript file pathname should match',
    );

    t.equal(
        obj.files[0].mimeType,
        'application/javascript',
        'JavaScript file mime should match',
    );

    t.equal(
        obj.files[1].pathname,
        '/biz/pkg/frazz/2.1.4/main/index.js.map',
        'JavaScript file source map pathname should match',
    );
    t.equal(
        obj.files[1].mimeType,
        'application/json',
        'JavaScript file source map mime should match',
    );

    t.equal(
        obj.files[2].pathname,
        '/biz/pkg/frazz/2.1.4/ie11/index.js',
        'ie11 fallback bundle pathname should match',
    );
    t.equal(
        obj.files[2].mimeType,
        'application/javascript',
        'ie11 fallback bundle mime should match',
    );

    t.equal(
        obj.files[3].pathname,
        '/biz/pkg/frazz/2.1.4/ie11/index.js.map',
        'ie11 fallback bundle source map pathname should match',
    );
    t.equal(
        obj.files[3].mimeType,
        'application/json',
        'ie11 fallback bundle source map mime should match',
    );

    t.equal(
        obj.files[4].pathname,
        '/biz/pkg/frazz/2.1.4/main/index.css',
        'css file pathname should match',
    );
    t.equal(obj.files[4].mimeType, 'text/css', 'css file mime should match');

    t.equal(
        obj.files[5].pathname,
        '/biz/pkg/frazz/2.1.4/main/index.css.map',
        'css file source map pathname should match',
    );
    t.equal(
        obj.files[5].mimeType,
        'application/json',
        'css file source map mime should match',
    );

    t.equal(
        obj.files[6].pathname,
        '/biz/pkg/frazz/2.1.4/assets.json',
        'assets.json pathname should match',
    );
    t.equal(
        obj.files[6].mimeType,
        'application/json',
        'assets.json mime should match',
    );

    await service.stop();
});

test('Alias GET', async t => {
    const sink = new SinkTest();
    const service = new FastifyService({ customSink: sink, port: 0, logger: false });
    const address = await service.start();

    sink.set(
        '/biz/pkg/fuzz/8.alias.json',
        JSON.stringify({
            pathname: '/biz/pkg/fuzz/8.4.1',
            version: '8.4.1',
            alias: '8',
            type: 'pkg',
            name: 'fuzz',
            org: 'biz',
        }),
    );
    sink.set('/biz/pkg/fuzz/8.4.1/main/index.js', 'hello world');

    const res = await fetch(
        `${address}/biz/pkg/fuzz/v8/main/index.js`,
    );
    const body = await res.text();
    t.equals(res.status, 200, 'server should respond with 200 ok');
    t.equals(body, 'hello world', 'server should respond with file contents');

    await service.stop();
});

test('Alias DELETE', async t => {
    const sink = new SinkTest();
    const service = new FastifyService({ customSink: sink, port: 0, logger: false });
    const address = await service.start();

    sink.set(
        '/biz/pkg/fuzz/8.alias.json',
        JSON.stringify({
            org: 'biz',
            type: 'pkg',
            name: 'fuzz',
            version: '8.4.1',
        }),
    );
    sink.set('/biz/pkg/fuzz/8.4.1/main/index.js', 'hello world');

    await fetch(`${address}/biz/pkg/fuzz/v8`, {
        method: 'DELETE',
    });

    const contents1 = sink.get('/biz/pkg/fuzz/8.alias.json');
    const contents2 = sink.get('/biz/pkg/fuzz/8.4.1/main/index.js');

    t.same(contents1, null, 'alias should have been deleted');
    t.same(contents2, 'hello world', 'aliased file should still be available');

    await service.stop();
});

test('Alias PUT', async t => {
    const sink = new SinkTest();
    const service = new FastifyService({ customSink: sink, port: 0, logger: false });
    const address = await service.start();

    sink.set('/biz/pkg/fuzz/8.4.1/main/index.js', 'hello world');

    const formData = new FormData();
    formData.append('version', '8.4.1');

    await fetch(`${address}/biz/pkg/fuzz/v8`, {
        method: 'PUT',
        body: formData,
        headers: formData.getHeaders(),
    });

    const contents = sink.get('/biz/pkg/fuzz/8.alias.json');

    t.same(
        contents,
        JSON.stringify({
            pathname: '/biz/pkg/fuzz/8.4.1',
            version: '8.4.1',
            alias: '8',
            type: 'pkg',
            name: 'fuzz',
            org: 'biz',
        }),
        'alias should have been created',
    );

    await service.stop();
});

test('Alias POST', async t => {
    const sink = new SinkTest();
    const service = new FastifyService({ customSink: sink, port: 0, logger: false });
    const address = await service.start();

    sink.set('/biz/pkg/fuzz/8.4.1/main/index.js', 'hello world');

    const formData = new FormData();
    formData.append('version', '8.4.1');

    await fetch(`${address}/biz/pkg/fuzz/v8`, {
        method: 'POST',
        body: formData,
        headers: formData.getHeaders(),
    });

    const contents = sink.get('/biz/pkg/fuzz/8.alias.json');

    t.same(
        contents,
        JSON.stringify({
            pathname: '/biz/pkg/fuzz/8.4.1',
            version: '8.4.1',
            alias: '8',
            type: 'pkg',
            name: 'fuzz',
            org: 'biz',
        }),
        'alias should have been created',
    );

    await service.stop();
});

test('Map GET', async t => {
    const sink = new SinkTest();
    const service = new FastifyService({ customSink: sink, port: 0, logger: false });
    const address = await service.start();

    sink.set(
        '/biz/map/buzz/4.2.2.json',
        JSON.stringify({
            imports: {
                fuzz: `${address}/finn/pkg/fuzz/v8`,
            },
        }),
    );

    const res = await fetch(`${address}/biz/map/buzz/4.2.2`);

    const content = await res.text();

    t.equal(res.status, 200, 'response status code should be 200 ok');
    t.same(
        content,
        JSON.stringify({
            imports: { fuzz: `${address}/finn/pkg/fuzz/v8` },
        }),
        'content should be an import map in JSON format',
    );

    await service.stop();
});

test('Map PUT', async t => {
    const sink = new SinkTest();
    const service = new FastifyService({ customSink: sink, port: 0, logger: false });
    const address = await service.start();

    const formData = new FormData();
    formData.append(
        'map',
        createReadStream(join(__dirname, '../../fixtures/import-map.json')),
        {}
    );

    const res = await fetch(`${address}/biz/map/buzz/4.2.2`, {
        method: 'PUT',
        body: formData,
        headers: formData.getHeaders(),
    });

    const content = sink.get('/biz/map/buzz/4.2.2.json');

    t.same(
        JSON.parse(content),
        {
            imports: { fuzz: 'http://localhost:4001/finn/pkg/fuzz/v8' },
        },
        'content should be an import map in JSON format',
    );

    t.equal(res.status, 200, 'response status code should be 200 ok');

    await service.stop();
});
