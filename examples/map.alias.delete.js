'use strict';

const fetch = require('node-fetch');

fetch('http://localhost:4001/biz/map/buzz/v4', {
    method: 'DELETE',
})
.then((res) => res.text())
.then(body => console.log('Alias deleted', body));