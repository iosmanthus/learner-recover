const shell = require('shelljs');

console.log(shell.exec('ls -la', { silent: true }).stdout);