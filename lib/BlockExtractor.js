'use strict';
var  digibyte  = require('digibyte'),
     Block    = digibyte.Block,
     networks = digibyte.Networks,
     Parser   = digibyte.encoding.BufferReader,
     fs       = require('fs'),
     glob     = require('glob'),
     async    = require('async');

function BlockExtractor(dataDir, network) {
  var path = dataDir + '/blocks/blk*.dat';

  this.dataDir = dataDir;
  this.files   = glob.sync(path);
  this.nfiles  = this.files.length;

  if (this.nfiles === 0)
    throw new Error('Could not find block files at: ' + path);

  this.currentFileIndex = 0;
  this.isCurrentRead    = false;
  this.currentBuffer    = null;
  this.currentParser    = null;
  this.network = network === 'testnet' ? networks.testnet: networks.livenet;
  this.magic   = this.network.networkMagic.toString('hex');
}

BlockExtractor.prototype.currentFile = function() {
  return this.files[this.currentFileIndex];
};


BlockExtractor.prototype.nextFile = function() {
  if (this.currentFileIndex < 0) return false;

  var ret  = true;

  this.isCurrentRead = false;
  this.currentBuffer = null;
  this.currentParser = null;

  if (this.currentFileIndex < this.nfiles - 1) {
    this.currentFileIndex++;
  }
  else {
    this.currentFileIndex=-1;
    ret = false;
  }
  return ret;
};

BlockExtractor.prototype.readCurrentFileSync = function() {
  if (this.currentFileIndex < 0 || this.isCurrentRead) return;

  this.isCurrentRead = true;

  var fname = this.currentFile();
  if (!fname) return;


  var stats = fs.statSync(fname);

  var size = stats.size;

  console.log('Reading Blockfile %s [%d MB]',
            fname, parseInt(size/1024/1024));

  var fd = fs.openSync(fname, 'r');

  var buffer = Buffer.alloc(size);

  fs.readSync(fd, buffer, 0, size, 0);

  this.currentBuffer = buffer;
  this.currentParser = new Parser(buffer);
};




BlockExtractor.prototype._getMagic = function() {
  if (!this.currentParser) 
    return null;

  //var byte0 = this.currentParser ? this.currentParser.buffer(1).toString('hex') : null;



  // Grab 3 bytes from block without removing them
  var p = this.currentParser.pos;
  var magic = this.currentParser.readUInt32BE().toString(16);
  if (magic !=='00000000' && magic !== this.magic) {
    if(this.errorCount++ > 4)
      throw new Error('CRITICAL ERROR: Magic number mismatch: ' +
                          magic + '!=' + this.magic);
    magic=null;
  }
  
  if (magic==='00000000') 
    magic =null;

  return magic;
};

BlockExtractor.prototype.getNextBlock = function(cb) {
  var b;
  var magic;
  var isFinished = 0;

  while(!magic && !isFinished)  {
    this.readCurrentFileSync();
    magic= this._getMagic();

    if (!this.currentParser || this.currentParser.eof() ) {

      if (this.nextFile()) {
        console.log('Moving forward to file:' + this.currentFile() );
        magic = null;
      } else {
        console.log('Finished all files');
        isFinished = 1;
      }
    }
  }
  if (isFinished)
    return cb();

  var blockSize = this.currentParser.readUInt32LE();
  var b = digibyte.Block.fromBufferReader (this.currentParser);
  this.errorCount=0;
  return cb(null,b);
};

module.exports = require('soop')(BlockExtractor);

