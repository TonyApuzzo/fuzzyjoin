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

public abstract class AbstractUTF8Token implements IToken {
	protected int length;
	protected int tokenLength;
	protected int start;
	protected int tokenCount;
	protected byte[] data;
	protected final byte tokenTypeTag;
	protected final byte countTypeTag;

	public AbstractUTF8Token() {
		tokenTypeTag = -1;
		countTypeTag = -1;
	}

	public AbstractUTF8Token(byte tokenTypeTag, byte countTypeTag) {
		this.tokenTypeTag = tokenTypeTag;
		this.countTypeTag = countTypeTag;
	}

	@Override
	public byte[] getData() {
		return data;
	}

	@Override
	public int getLength() {
		return length;
	}

	@Override
	public int getStart() {
		return start;
	}

	@Override
	public int getTokenLength() {
		return tokenLength;
	}

	public void handleCountTypeTag(DataOutput dos) throws IOException {
		if (countTypeTag > 0) {
			dos.write(countTypeTag);
		}
	}

	public void handleTokenTypeTag(DataOutput dos) throws IOException {
		if (tokenTypeTag > 0) {
			dos.write(tokenTypeTag);
		}
	}

	@Override
	public void reset(byte[] data, int start, int length, int tokenLength,
			int tokenCount) {
		this.data = data;
		this.start = start;
		this.length = length;
		this.tokenLength = tokenLength;
		this.tokenCount = tokenCount;
	}

	@Override
	public void serializeTokenCount(DataOutput dos) throws IOException {
		handleCountTypeTag(dos);
		dos.writeInt(tokenCount);
	}
}
