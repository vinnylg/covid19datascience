var fs = require('fs')

var obj = JSON.parse(fs.readFileSync('file', 'utf8'))
console.log(obj)