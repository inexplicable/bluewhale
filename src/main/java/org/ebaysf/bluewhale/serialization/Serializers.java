package org.ebaysf.bluewhale.serialization;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtobufIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.primitives.Chars;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.logging.Logger;

public abstract class Serializers {

	private static final Logger LOG = Logger.getLogger(Serializer.class.getName());
	
	public static abstract class AbstractBinComparableSerializer<E> implements Serializer<E> {
		
		public @Override boolean equals(final ByteBuffer target, final ByteBuffer other) {

            Preconditions.checkNotNull(target);
			Preconditions.checkNotNull(other);
			
			return Objects.equal(target.duplicate(), other.duplicate());
		}

        public @Override boolean equals(final E target, final E other){

            Preconditions.checkNotNull(target);
            Preconditions.checkNotNull(other);

            return Objects.equal(target, other);
        }

        public @Override boolean equals(final E target, final ByteBuffer other){

            Preconditions.checkNotNull(target);
            Preconditions.checkNotNull(other);

            return Objects.equal(serialize(target).duplicate(), other.duplicate());
        }

        public @Override boolean equals(final ByteBuffer target, final E other){

            Preconditions.checkNotNull(target);
            Preconditions.checkNotNull(other);

            return Objects.equal(target.duplicate(), serialize(other).duplicate());
        }
		
		public @Override int hashCode(final E object){

			return object.hashCode();
		}
	}
	
	public static abstract class AbstractProtostuffSerializer<E> implements Serializer<E> {
		
		private final Schema<E> _schema;
		
		public AbstractProtostuffSerializer(Class<E> clazz){
			_schema = RuntimeSchema.getSchema(clazz);
		}
		
		public ByteBuffer serialize(final E object) {
			final ByteArrayOutputStream bos = new ByteArrayOutputStream();
			try {
				ProtobufIOUtil.writeTo(bos, object, _schema, LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE));
			}
			catch (IOException e) {
				LOG.warning(Throwables.getStackTraceAsString(e));
			}	
			return ByteBuffer.wrap(bos.toByteArray());
		}

		public E deserialize(final ByteBuffer binaries, final boolean compressed) {
			try {
				final ByteBuffer protobuf = compressed ? uncompress(binaries) : binaries.duplicate();
				final E object = _schema.newMessage();
				if(!protobuf.isDirect()){
					ProtobufIOUtil.mergeFrom(protobuf.array(), protobuf.position(), protobuf.remaining(), object, _schema);
				}
				else{
					ProtobufIOUtil.mergeFrom(new ByteBufferAsInputStream(protobuf), object, _schema);
				}
				return object;
			} 
			catch (IOException e) {
				LOG.warning(Throwables.getStackTraceAsString(e));
			}
			return null;
		}
		
		public @Override boolean equals(final ByteBuffer target, final ByteBuffer other) {

            Preconditions.checkNotNull(target);
			Preconditions.checkNotNull(other);
			
			return Objects.equal(deserialize(target, false), deserialize(other, false));
		}

        public @Override boolean equals(final E target, final E other){

            Preconditions.checkNotNull(target);
            Preconditions.checkNotNull(other);

            return Objects.equal(target, other);
        }

        public @Override boolean equals(final E target, final ByteBuffer other){

            Preconditions.checkNotNull(target);
            Preconditions.checkNotNull(other);

            return Objects.equal(target, deserialize(other.duplicate(), false));
        }

        public @Override boolean equals(final ByteBuffer target, final E other){

            Preconditions.checkNotNull(target);
            Preconditions.checkNotNull(other);

            return Objects.equal(deserialize(target.duplicate(), false), other);
        }

		public @Override int hashCode(final E object){
			return object.hashCode();
		}
	}
	
	public static ByteBuffer compress(final ByteBuffer origin) throws IOException{
        if(!origin.isDirect()){
            final byte[] compressed = new byte[Snappy.maxCompressedLength(origin.remaining())];
            final int length = Snappy.compress(origin.array(), origin.position(), origin.remaining(), compressed, 0);
            return ByteBuffer.wrap(compressed, 0, length);
        }
        else{
            final byte[] uncompressed = new byte[origin.remaining()];
            origin.duplicate().get(uncompressed, 0, uncompressed.length);
            return ByteBuffer.wrap(Snappy.compress(uncompressed));
        }
	}
	
	public static ByteBuffer uncompress(final ByteBuffer compressed) throws IOException{
        if(!compressed.isDirect()){
            final byte[] uncompressed = new byte[Snappy.uncompressedLength(compressed.array(), compressed.position(), compressed.remaining())];
            final int length = Snappy.uncompress(compressed.array(), compressed.position(), compressed.remaining(), uncompressed, 0);
            return ByteBuffer.wrap(uncompressed, 0, length);
        }
        else{
            final byte[] compressedAsBytes = new byte[compressed.remaining()];
            compressed.duplicate().get(compressedAsBytes, 0, compressedAsBytes.length);
            return ByteBuffer.wrap(Snappy.uncompress(compressedAsBytes));
        }
	}

	public static final Serializer<Boolean> BOOL_SERIALIZER = new AbstractBinComparableSerializer<Boolean>() {
		public final ByteBuffer FALSE = ByteBuffer.wrap(new byte[]{(byte)0});
		public final ByteBuffer TRUE = ByteBuffer.wrap(new byte[]{(byte)1});
		
		public @Override ByteBuffer serialize(Boolean object) {
			return object.booleanValue() ? TRUE.duplicate() : FALSE.duplicate();
		}

		public @Override Boolean deserialize(ByteBuffer binaries, final boolean compressed) {
			try {
				binaries = compressed ? uncompress(binaries) : binaries;
			} 
			catch (IOException e) {
				LOG.warning(Throwables.getStackTraceAsString(e));
			}
			return binaries.duplicate().equals(TRUE.duplicate()) ? Boolean.TRUE : Boolean.FALSE;
		}
	};
	
	public static final Serializer<Byte> BYTE_SERIALIZER = new AbstractBinComparableSerializer<Byte>() {

		public @Override ByteBuffer serialize(final Byte object) {
			return ByteBuffer.wrap(new byte[]{object.byteValue()});
		}

		public @Override Byte deserialize(ByteBuffer binaries, final boolean compressed) {
			try {
				binaries = compressed ? uncompress(binaries) : binaries.duplicate();
			} 
			catch (IOException e) {
				LOG.warning(Throwables.getStackTraceAsString(e));
			}
			return Byte.valueOf(binaries.get());
		}
	};
	
	public static final Serializer<Short> SHORT_SERIALIZER = new AbstractBinComparableSerializer<Short>() {

		public @Override ByteBuffer serialize(final Short object) {
            final ByteBuffer buffer = ByteBuffer.allocate(Shorts.BYTES);
            buffer.putShort(object);
            buffer.rewind();
            return buffer;
		}

		public @Override Short deserialize(ByteBuffer binaries, final boolean compressed) {
			try {
				binaries = compressed ? uncompress(binaries) : binaries.duplicate();
			} 
			catch (IOException e) {
				LOG.warning(Throwables.getStackTraceAsString(e));
			}
			return binaries.getShort();
		}
	};
	
	public static final Serializer<Integer> INT_SERIALIZER = new AbstractBinComparableSerializer<Integer>() {

		public @Override ByteBuffer serialize(final Integer object) {
            final ByteBuffer buffer = ByteBuffer.allocate(Ints.BYTES);
            buffer.putInt(object);
            buffer.rewind();
			return buffer;
		}

		public @Override Integer deserialize(ByteBuffer binaries, final boolean compressed) {
			try {
				binaries = compressed ? uncompress(binaries) : binaries.duplicate();
			} 
			catch (IOException e) {
				LOG.warning(Throwables.getStackTraceAsString(e));
			}
			return binaries.getInt();
		}
	};
	
	public static final Serializer<Long> LONG_SERIALIZER = new AbstractBinComparableSerializer<Long>() {

		public @Override ByteBuffer serialize(final Long object) {
            final ByteBuffer buffer = ByteBuffer.allocate(Longs.BYTES);
            buffer.putLong(object);
            buffer.rewind();
			return buffer;
		}

		public @Override Long deserialize(ByteBuffer binaries, final boolean compressed) {
			try {
				binaries = compressed ? uncompress(binaries) : binaries.duplicate();
			} 
			catch (IOException e) {
				LOG.warning(Throwables.getStackTraceAsString(e));
			}
			return binaries.getLong();
		}
	};
	
	public static final Serializer<Character> CHAR_SERIALIZER = new AbstractBinComparableSerializer<Character>() {

		public @Override ByteBuffer serialize(final Character object) {
            final ByteBuffer buffer = ByteBuffer.allocate(Chars.BYTES);
            buffer.putChar(object.charValue());
			buffer.rewind();
            return buffer;
		}

		public @Override Character deserialize(ByteBuffer binaries, final boolean compressed) {
			try {
				binaries = compressed ? uncompress(binaries) : binaries.duplicate();
			} 
			catch (IOException e) {
				LOG.warning(Throwables.getStackTraceAsString(e));
			}
			return Character.valueOf(binaries.getChar());
		}
	};
	
	public static final Serializer<String> STRING_SERIALIZER = new AbstractBinComparableSerializer<String>() {

		public @Override ByteBuffer serialize(final String object) {
			Preconditions.checkNotNull(object);
			
			return ByteBuffer.wrap(object.getBytes(Charsets.UTF_8));
		}

		public @Override String deserialize(ByteBuffer binaries, final boolean compressed) {
			try {
				binaries = compressed ? uncompress(binaries) : binaries.duplicate();
			} 
			catch (IOException e) {
				LOG.warning(Throwables.getStackTraceAsString(e));
			}
            if(!binaries.isDirect()){
			    return new String(binaries.array(), binaries.position(), binaries.remaining(), Charsets.UTF_8);
            }
            else{
                final byte[] copy = new byte[binaries.remaining()];
                binaries.get(copy, 0, copy.length);
                return new String(copy, Charsets.UTF_8);
            }
		}
	};
	
	public static final Serializer<byte[]> BYTE_ARRAY_SERIALIZER = new AbstractBinComparableSerializer<byte[]>() {

		public @Override ByteBuffer serialize(final byte[] object) {
			Preconditions.checkNotNull(object);
			
			return ByteBuffer.wrap(object);
		}

		public @Override byte[] deserialize(ByteBuffer binaries, final boolean compressed) {
			try {
				binaries = compressed ? uncompress(binaries) : binaries.duplicate();
			} 
			catch (IOException e) {
				LOG.warning(Throwables.getStackTraceAsString(e));
			}
            if(!binaries.isDirect()){
			    return Arrays.copyOfRange(binaries.array(), binaries.position(), binaries.position() + binaries.remaining());
            }
            else{
                final byte[] copy = new byte[binaries.remaining()];
                binaries.get(copy, 0, copy.length);
                return copy;
            }
		}
		
		public @Override int hashCode(final byte[] object){
			return Arrays.hashCode(object);
		}
	};

	public static final int BYTE_MASK = (1 << 9) - 1;
	
	public static class ByteBufferAsInputStream extends InputStream{

		private final ByteBuffer _buffer;
		
		public ByteBufferAsInputStream(final ByteBuffer buffer){
			_buffer = buffer.duplicate();
		}
		
		public @Override int read() throws IOException {
			if(_buffer.hasRemaining()){
				return BYTE_MASK & _buffer.get();
			}
			return -1;
		}
	}
}
