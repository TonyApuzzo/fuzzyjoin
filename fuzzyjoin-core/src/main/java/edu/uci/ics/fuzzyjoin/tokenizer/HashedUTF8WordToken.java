/**
 * Copyright 2010-2011 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS"; BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under
 * the License.
 * 
 * Author: Alexander Behm <abehm (at) ics.uci.edu>
 */

package edu.uci.ics.fuzzyjoin.tokenizer;

import java.io.DataOutput;
import java.io.IOException;

public class HashedUTF8WordToken extends UTF8WordToken {

	private int hash = 0;

	public HashedUTF8WordToken(byte tokenTypeTag, byte countTypeTag) {
		super(tokenTypeTag, countTypeTag);
	}

	@Override
	public boolean equals(Object o) {
		if (o == null) {
			return false;
		}
		if (!(o instanceof IToken)) {
			return false;
		}
		IToken t = (IToken) o;
		if (t.getTokenLength() != tokenLength) {
			return false;
		}
		int offset = 0;
		for (int i = 0; i < tokenLength; i++) {
			if (StringUtils.charAt(t.getData(), t.getStart() + offset) != StringUtils
					.charAt(data, start + offset)) {
				return false;
			}
			offset += StringUtils.charSize(data, start + offset);
		}
		return true;
	}

	@Override
	public int hashCode() {
		return hash;
	}

	@Override
	public void reset(byte[] data, int start, int length, int tokenLength,
			int tokenCount) {
		super.reset(data, start, length, tokenLength, tokenCount);

		// pre-compute hash value
		int pos = start;
		hash = 0;
		for (int i = 0; i < tokenLength; i++) {
			hash = 31 * hash
					+ StringUtils.toLowerCase(StringUtils.charAt(data, pos));
			pos += StringUtils.charSize(data, pos);
		}
		hash = 31 * hash + tokenCount;
	}

	@Override
	public void serializeToken(DataOutput dos) throws IOException {
		if (tokenTypeTag > 0) {
			dos.write(tokenTypeTag);
		}

		// serialize hash value
		dos.writeInt(hash);
	}
}
