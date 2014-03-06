package org.ebaysf.bluewhale.document;

import com.google.common.base.Objects;

import java.nio.ByteBuffer;

public class BinDocumentRaw implements BinDocument {

    private ByteBuffer _key;

    private ByteBuffer _val;

    private int _hashCode;

    private long _next;

    private long _lastModified;

    private byte _state = 0x00;

    public @Override ByteBuffer getKey() {
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
                    && _next == that.getNext()
                    && Objects.equal(getKey(), that.getKey())
                    && Objects.equal(getValue(), that.getValue())
                    && _lastModified == that.getLastModified()
                    && _state == that.getState();
        }
        return false;
    }

    private static final int CONSTANT_LENGTH = BYTES_OF_INT //length of `key` buffer, highest byte as for `state`
            + BYTES_OF_INT //length of `value' buffer
            + BYTES_OF_LONG //next token
            + BYTES_OF_INT  //hashCode
            + BYTES_OF_LONG; //last modified

    public static int getLength(final int keyLength, final int valLength) {

        return CONSTANT_LENGTH
                + keyLength //bytes of `key`
                + valLength; //bytes of `value`
    }
}
