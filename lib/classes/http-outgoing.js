import { stream } from '@eik/common-utils';

const STATUS_CODES = [
    100, 101, 102, 103,
    200, 201, 202, 203, 204, 205, 206, 207, 208, 226,
    300, 301, 302, 303, 304, 305, 306, 307, 308,
    400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417, 418, 421, 422, 423, 424, 425, 426, 428, 429, 431, 451,
    500, 501, 502, 503, 504, 505, 506, 507, 508, 509, 510, 511,
];

const HttpOutgoing = class HttpOutgoing {
    constructor() {
        this._cacheControl = 'no-store';
        this._statusCode = 200;
        this._mimeType = 'text/plain';
        this._location = '';
        this._stream = undefined;
        this._body = undefined;
        this._etag = '';
    }

    set cacheControl(value) {
        if (value) {
            this._cacheControl = value;
        }
    }

    get cacheControl() {
        return this._cacheControl;
    }

    set statusCode(value) {
        if (Number.isInteger(value) && STATUS_CODES.includes(value)) {
            this._statusCode = value;
            return;
        }
        throw new Error('Value is not a legal http status code');
    }

    get statusCode() {
        return this._statusCode;
    }

    set location(value) {
        this._location = value;
    }

    get location() {
        return this._location;
    }

    set mimeType(value) {
        this._mimeType = value;
    }

    get mimeType() {
        return this._mimeType;
    }

    set stream(value) {
        if (!stream.isReadableStream(value)) throw new Error('Value is not a Readable stream');
        this._stream = value;
    }

    get stream() {
        return this._stream;
    }

    set body(value) {
        this._body = value;
    }

    get body() {
        return this._body;
    }

    set etag(value) {
        this._etag = value;
    }

    get etag() {
        return this._etag;
    }

    get [Symbol.toStringTag]() {
        return 'HttpOutgoing';
    }
}
export default HttpOutgoing;
