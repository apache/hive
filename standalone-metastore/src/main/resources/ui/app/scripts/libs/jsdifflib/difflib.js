/**
  * Copyright 2017 Hortonworks.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *   http://www.apache.org/licenses/LICENSE-2.0
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
**/

var __whitespace = {" ":true, "\t":true, "\n":true, "\f":true, "\r":true};

var difflib = {
  defaultJunkFunction: function (c) {
  	return __whitespace.hasOwnProperty(c);
  },

  stripLinebreaks: function (str) { return str.replace(/^[\n\r]*|[\n\r]*$/g, ""); },

  stringAsLines: function (str) {
  	var lfpos = str.indexOf("\n");
  	var crpos = str.indexOf("\r");
  	var linebreak = ((lfpos > -1 && crpos > -1) || crpos < 0) ? "\n" : "\r";

  	var lines = str.split(linebreak);
  	for (var i = 0; i < lines.length; i++) {
  		lines[i] = difflib.stripLinebreaks(lines[i]);
  	}

  	return lines;
  },

  // iteration-based reduce implementation
  __reduce: function (func, list, initial) {
  	if (initial != null) {
  		var value = initial;
  		var idx = 0;
  	} else if (list) {
  		var value = list[0];
  		var idx = 1;
  	} else {
  		return null;
  	}

  	for (; idx < list.length; idx++) {
  		value = func(value, list[idx]);
  	}

    return value;
  },

  // comparison function for sorting lists of numeric tuples
  __ntuplecomp: function (a, b) {
  	var mlen = Math.max(a.length, b.length);
  	for (var i = 0; i < mlen; i++) {
  		if (a[i] < b[i]) {return -1;}
  		if (a[i] > b[i]) {return 1;}
  	}
  
  	return a.length == b.length ? 0 : (a.length < b.length ? -1 : 1);
  },
  
  __calculate_ratio: function (matches, length) {
  	return length ? 2.0 * matches / length : 1.0;
  },
  
  // returns a function that returns true if a key passed to the returned function
  // is in the dict (js object) provided to this function; replaces being able to
  // carry around dict.has_key in python...
  __isindict: function (dict) {
  	return function (key) { return dict.hasOwnProperty(key); };
  },
  
  // replacement for python's dict.get function -- need easy default values
  __dictget: function (dict, key, defaultValue) {
  	return dict.hasOwnProperty(key) ? dict[key] : defaultValue;
  },

  SequenceMatcher: function (a, b, isjunk) {
    this.set_seqs = function (a, b) {
    	this.set_seq1(a);
    	this.set_seq2(b);
    };

    this.set_seq1 = function (a) {
    	if (a == this.a) {return;}
    	this.a = a;
    	this.matching_blocks = this.opcodes = null;
    };

    this.set_seq2 = function (b) {
    	if (b == this.b) {return;}
    	this.b = b;
    	this.matching_blocks = this.opcodes = this.fullbcount = null;
    	this.__chain_b();
    };

    this.__chain_b = function () {
      var b = this.b;
      var n = b.length;
      var b2j = this.b2j = {};
      var populardict = {};
      for (var i = 0; i < b.length; i++) {
      	var elt = b[i];
      	if (b2j.hasOwnProperty(elt)) {
      		var indices = b2j[elt];
      		if (n >= 200 && indices.length * 100 > n) {
      			populardict[elt] = 1;
      			delete b2j[elt];
      		} else {
      			indices.push(i);
      		}
      	} else {
      		b2j[elt] = [i];
      	}
      }

      for (var elt in populardict) {
      	if (populardict.hasOwnProperty(elt)) {
      		delete b2j[elt];
      	}
      }

      var isjunk = this.isjunk;
      var junkdict = {};
      if (isjunk) {
        for (var elt in populardict) {
          if (populardict.hasOwnProperty(elt) && isjunk(elt)) {
            junkdict[elt] = 1;
            delete populardict[elt];
          }
        }
        for (var elt in b2j) {
          if (b2j.hasOwnProperty(elt) && isjunk(elt)) {
            junkdict[elt] = 1;
            delete b2j[elt];
          }
        }
      }

      this.isbjunk = difflib.__isindict(junkdict);
      this.isbpopular = difflib.__isindict(populardict);
    };

    this.find_longest_match = function (alo, ahi, blo, bhi) {
      var a = this.a;
      var b = this.b;
      var b2j = this.b2j;
      var isbjunk = this.isbjunk;
      var besti = alo;
      var bestj = blo;
      var bestsize = 0;
      var j = null;
      var k;

      var j2len = {};
      var nothing = [];
      for (var i = alo; i < ahi; i++) {
      	var newj2len = {};
      	var jdict = difflib.__dictget(b2j, a[i], nothing);
      	for (var jkey in jdict) {
      		if (jdict.hasOwnProperty(jkey)) {
      			j = jdict[jkey];
      			if (j < blo) {continue;}
      			if (j >= bhi) {break;}
      			newj2len[j] = k = difflib.__dictget(j2len, j - 1, 0) + 1;
      			if (k > bestsize) {
      				besti = i - k + 1;
      				bestj = j - k + 1;
      				bestsize = k;
      			}
      		}
      	}
      	j2len = newj2len;
      }

      while (besti > alo && bestj > blo && !isbjunk(b[bestj - 1]) && a[besti - 1] == b[bestj - 1]) {
      	besti--;
      	bestj--;
      	bestsize++;
      }

      while (besti + bestsize < ahi && bestj + bestsize < bhi &&
      		!isbjunk(b[bestj + bestsize]) &&
      		a[besti + bestsize] == b[bestj + bestsize]) {
      	bestsize++;
      }

      while (besti > alo && bestj > blo && isbjunk(b[bestj - 1]) && a[besti - 1] == b[bestj - 1]) {
      	besti--;
      	bestj--;
      	bestsize++;
      }

      while (besti + bestsize < ahi && bestj + bestsize < bhi && isbjunk(b[bestj + bestsize]) &&
      		a[besti + bestsize] == b[bestj + bestsize]) {
      	bestsize++;
      }

    	return [besti, bestj, bestsize];
    };

    this.get_matching_blocks = function () {
      if (this.matching_blocks != null) {return this.matching_blocks;}
      var la = this.a.length;
      var lb = this.b.length;

      var queue = [[0, la, 0, lb]];
      var matching_blocks = [];
      var alo, ahi, blo, bhi, qi, i, j, k, x;
      while (queue.length) {
      	qi = queue.pop();
      	alo = qi[0];
      	ahi = qi[1];
      	blo = qi[2];
      	bhi = qi[3];
      	x = this.find_longest_match(alo, ahi, blo, bhi);
      	i = x[0];
      	j = x[1];
      	k = x[2];

      	if (k) {
      		matching_blocks.push(x);
      		if (alo < i && blo < j) {
      			queue.push([alo, i, blo, j]);
      }
      		if (i+k < ahi && j+k < bhi) {
      			queue.push([i + k, ahi, j + k, bhi]);
      }
      	}
      }

      matching_blocks.sort(difflib.__ntuplecomp);

      var i1 = 0, j1 = 0, k1 = 0, block = 0;
      var i2, j2, k2;
      var non_adjacent = [];
      for (var idx in matching_blocks) {
      	if (matching_blocks.hasOwnProperty(idx)) {
      		block = matching_blocks[idx];
      		i2 = block[0];
      		j2 = block[1];
      		k2 = block[2];
      		if (i1 + k1 == i2 && j1 + k1 == j2) {
      			k1 += k2;
      		} else {
      			if (k1) {non_adjacent.push([i1, j1, k1]);}
      			i1 = i2;
      			j1 = j2;
      			k1 = k2;
      		}
      	}
      }

      if (k1) {non_adjacent.push([i1, j1, k1]);}

    	non_adjacent.push([la, lb, 0]);
    	this.matching_blocks = non_adjacent;
    	return this.matching_blocks;
    };

    this.get_opcodes = function () {
      if (this.opcodes != null) {return this.opcodes;}
      var i = 0;
      var j = 0;
      var answer = [];
      this.opcodes = answer;
      var block, ai, bj, size, tag;
      var blocks = this.get_matching_blocks();
      for (var idx in blocks) {
      	if (blocks.hasOwnProperty(idx)) {
      		block = blocks[idx];
      		ai = block[0];
      		bj = block[1];
      		size = block[2];
      		tag = '';
      		if (i < ai && j < bj) {
      			tag = 'replace';
      		} else if (i < ai) {
      			tag = 'delete';
      		} else if (j < bj) {
      			tag = 'insert';
      		}
      		if (tag) {answer.push([tag, i, ai, j, bj]);}
      		i = ai + size;
      		j = bj + size;
      
      		if (size) {answer.push(['equal', ai, i, bj, j]);}
      	}
      }

    	return answer;
    };

    // this is a generator function in the python lib, which of course is not supported in javascript
    // the reimplementation builds up the grouped opcodes into a list in their entirety and returns that.
    this.get_grouped_opcodes = function (n) {
      if (!n) {n = 3;}
      var codes = this.get_opcodes();
      if (!codes) {codes = [["equal", 0, 1, 0, 1]];}
      var code, tag, i1, i2, j1, j2;
      if (codes[0][0] == 'equal') {
      	code = codes[0];
      	tag = code[0];
      	i1 = code[1];
      	i2 = code[2];
      	j1 = code[3];
      	j2 = code[4];
      	codes[0] = [tag, Math.max(i1, i2 - n), i2, Math.max(j1, j2 - n), j2];
      }
      if (codes[codes.length - 1][0] == 'equal') {
      	code = codes[codes.length - 1];
      	tag = code[0];
      	i1 = code[1];
      	i2 = code[2];
      	j1 = code[3];
      	j2 = code[4];
      	codes[codes.length - 1] = [tag, i1, Math.min(i2, i1 + n), j1, Math.min(j2, j1 + n)];
      }

    	var nn = n + n;
    	var group = [];
    	var groups = [];
    	for (var idx in codes) {
    		if (codes.hasOwnProperty(idx)) {
    			code = codes[idx];
    			tag = code[0];
    			i1 = code[1];
    			i2 = code[2];
    			j1 = code[3];
    			j2 = code[4];
    			if (tag == 'equal' && i2 - i1 > nn) {
    				group.push([tag, i1, Math.min(i2, i1 + n), j1, Math.min(j2, j1 + n)]);
    				groups.push(group);
    				group = [];
    				i1 = Math.max(i1, i2-n);
    				j1 = Math.max(j1, j2-n);
    			}

      		group.push([tag, i1, i2, j1, j2]);
      	}
    }

      if (group && !(group.length == 1 && group[0][0] == 'equal')) {groups.push(group);}

      return groups;
    };

    this.ratio = function () {
    	matches = difflib.__reduce(
    					function (sum, triple) { return sum + triple[triple.length - 1]; },
    					this.get_matching_blocks(), 0);
    	return difflib.__calculate_ratio(matches, this.a.length + this.b.length);
    };

    this.quick_ratio = function () {
      var fullbcount, elt;
      if (this.fullbcount == null) {
      	this.fullbcount = fullbcount = {};
      	for (var i = 0; i < this.b.length; i++) {
      		elt = this.b[i];
      		fullbcount[elt] = difflib.__dictget(fullbcount, elt, 0) + 1;
      	}
      }
      fullbcount = this.fullbcount;

      var avail = {};
      var availhas = difflib.__isindict(avail);
      var matches = numb = 0;
      for (var i = 0; i < this.a.length; i++) {
      	elt = this.a[i];
      	if (availhas(elt)) {
      		numb = avail[elt];
      	} else {
      		numb = difflib.__dictget(fullbcount, elt, 0);
      	}
      	avail[elt] = numb - 1;
      	if (numb > 0) {matches++;}
      }

      return difflib.__calculate_ratio(matches, this.a.length + this.b.length);
    };

    this.real_quick_ratio = function () {
    	var la = this.a.length;
    	var lb = this.b.length;
    	return _calculate_ratio(Math.min(la, lb), la + lb);
    };

    this.isjunk = isjunk ? isjunk : difflib.defaultJunkFunction;
    this.a = this.b = null;
    this.set_seqs(a, b);
  }
};

module.exports = {difflib: difflib};