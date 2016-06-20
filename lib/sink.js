"use strict";

const stream    = require('readable-stream'),
      isObject  = require('lodash.isobject'),
      isString  = require('lodash.isstring'),
      crypto    = require('crypto'),
      assert    = require('assert'),
      AWS       = require('aws-sdk'),
      path      = require('path'),
      fs        = require('fs');



// http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/Config.html#constructor-property

const SinkS3 = module.exports = function (options) {
    if (!(this instanceof SinkS3)) return new SinkS3(options);

    assert(options && isObject(options), '"options" object must be provided');
    assert(options.key && isString(options.key), '"options.key" must be provided');
    assert(options.secret && isString(options.secret), '"options.secret" must be provided');
    assert(options.bucket && isString(options.bucket), '"options.bucket" must be provided');
    assert(options.endpoint && isString(options.endpoint), '"options.endpoint" must be provided');

    this.options = options;
    this.s3TmpDir = 'upload-temp-files';
};



SinkS3.prototype.createFilePath = function () {
    let now = new Date();
    let year = now.getFullYear().toString();
    let month = (now.getMonth() + 1).toString();
    let day = now.getDate().toString();
    return year + '/' + month + '/' + day + '/';
};



SinkS3.prototype.createTempFileName = function (fileType) {
    let rand = Math.floor(Math.random() * 1000).toString();
    return this.s3TmpDir + '/tmp-' + Date.now().toString() + '-' + rand + '.' + fileType;
}



SinkS3.prototype.writer = function (fileType, callback) {
    let temp = this.createTempFileName(fileType);
    let hash = crypto.createHash('sha1');
    let s3 = new AWS.S3({
        signatureVersion: 'v2',
        accessKeyId: this.options.key,
        secretAccessKey: this.options.secret,
        endpoint: this.options.endpoint,
        params: {
            Bucket: this.options.bucket, 
            Key: temp
        }
    });


    let hasher = new stream.Transform({
        transform: function (chunk, encoding, next) {
            hash.update(chunk, 'utf8');
            this.push(chunk);
            next();
        }
    });


    s3.upload({Body: hasher}).send((error, data) => { 
        if (error) return callback(error);
        let source = this.options.bucket + '/' + temp;
        let dest = this.createFilePath() + hash.digest('hex') + '.' + fileType;

        s3.copyObject({CopySource: source, Key: dest}, (err, data) => {
            if (err) return callback(err);

            s3.deleteObject({Key: temp}, (er, dat) => {
                if (er) return callback(er)
                callback(null, dest);
            });
        });
    });

    return hasher;
};



SinkS3.prototype.reader = function (file, callback) {

    let s3 = new AWS.S3({
        signatureVersion: 'v2',
        accessKeyId: this.options.key,
        secretAccessKey: this.options.secret,
        endpoint: this.options.endpoint,
        params: {
            Bucket: this.options.bucket, 
            Key: file
        }
    });

    let reader = s3.getObject().createReadStream();
    
    reader.on('finish', () => {
        callback();
    });

    return reader;
};
