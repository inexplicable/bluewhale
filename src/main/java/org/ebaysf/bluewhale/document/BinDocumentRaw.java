package org.ebaysf.bluewhale.document;

import com.dyuproject.protostuff.Tag;
import com.google.common.base.Objects;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class BinDocumentRaw implements BinDocument {

    @Tag(2)
    private ByteBuffer _key;

    @Tag(3)
    private ByteBuffer _val;

    @Tag(4)
    private int _hashCode;

    @Tag(1)
    private long _next;

    @Tag(5)
    private long _lastModified;

    @Tag(6)
    private byte _state = 0x00;

    @Override
    public ByteBuffer getKey() {
        return _key.duplicate();
    }

    public BinDocumentRaw setKey(final ByteBuffer key){
        _key = key.duplicate();
        return this;
    }

    public @Override ByteBuffer getValue() {
        return _val.duplicate();
    }

    public BinDocumentRaw setValue(final ByteBuffer val){
        _val = val.duplicate();
        return this;
    }

    public @Override int getHashCode() {
        return _hashCode;
    }

    public BinDocumentRaw setHashCode(final int hashCode){
        _hashCode = hashCode;
        return this;
    }

    public @Override long getNext() {
        return _next;
    }

    public BinDocumentRaw setNext(final long next){
        _next = next;
        return this;
    }

    public @Override long getLastModified() {
        return _lastModified;
    }

    public BinDocumentRaw setLastModified(final long lastModified){
        _lastModified = lastModified;
        return this;
    }

    public @Override int getLength(){
        return getLength(_key.remaining(), _val.remaining());
    }

    public @Override byte getState() {
        return _state;
    }

    public @Override int hashCode(){
        return Objects.hashCode(_hashCode, _next, _lastModified, _state);
    }

    public @Override boolean isTombstone() {
        return (_state & TOMBSTONE) != 0;
    }

    public @Override boolean isCompressed() {
        return (_state & COMPRESSED) != 0;
    }

    public BinDocumentRaw setState(final byte state){
        _state |= state;
        return this;
    }

    public @Override boolean equals(Object other){

        if(other instanceof BinDocument){
            final BinDocument that = (BinDocument)other;
            return _hashCode == that.getHashCode()
                    && Objects.equal(getKey(), that.getKey())
                    && Objects.equal(getValue(), that.getValue())
                    && _next == that.getNext()
                    && _lastModified == that.getLastModified()
                    && _state == that.getState();
        }
        return false;
    }

    public static int getLength(final int keyLength, final int valLength) {

        return BYTES_OF_INT //length of `key` buffer, highest byte as for `state`
                + BYTES_OF_INT //length of `value' buffer
                + BYTES_OF_LONG //next token
                + BYTES_OF_INT  //hashCode
                + keyLength //bytes of `key`
                + valLength //bytes of `value`
                + BYTES_OF_LONG //last modified
                + BYTES_OF_LONG;//checksum
    }

    public static long getChecksum(final ByteBuffer buffer, final int offset, final int length) {

        final CRC32 crc32 = new CRC32();
        crc32.update(buffer.array(), offset, length);
        return crc32.getValue();
    }
}
