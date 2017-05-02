package net.intelie.disq;

import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Base64;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;

public class DefaultSerializerTest {
    @Test
    public void willReadAndWrite() throws Exception {
        DefaultSerializer<String> serializer = new DefaultSerializer<>();

        Buffer buffer = new Buffer();
        serializer.serialize(buffer.write(), "test");
        assertThat(buffer.count()).isEqualTo(11);

        assertThat(serializer.deserialize(buffer.read())).isEqualTo("test");
    }

    @Test
    public void willCompressOkay() throws Exception {
        String s = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Phasellus ac magna accumsan, tempus lorem sit amet, consequat diam. Sed placerat sagittis neque. Suspendisse sit amet pulvinar nulla. Fusce in ligula in ante auctor gravida eget vitae libero. Suspendisse eu gravida justo. Nam imperdiet, lacus ac euismod aliquam, augue ligula consequat felis, et sagittis eros augue a orci. Nulla odio neque, dictum ornare euismod ut, faucibus ac augue. Nullam justo justo, aliquam in quam non, tincidunt tincidunt libero. In suscipit sapien eu tortor dapibus, in laoreet leo vestibulum. Sed malesuada ante metus, sed imperdiet nisl rutrum non. Pellentesque elementum facilisis quam, at imperdiet diam viverra eget. Donec pharetra lobortis elementum. Vivamus lobortis tortor nec posuere ornare.";

        DefaultSerializer<String> serializer = new DefaultSerializer<>(true);

        Buffer buffer = new Buffer();
        serializer.serialize(buffer.write(), s);

        assertThat(serializer.deserialize(buffer.read())).isEqualTo(s);
    }

    @Test
    public void willFailIfClassNotFound() throws Exception {
        byte[] bytes = Base64.getDecoder().decode("eJxb85aBtbiIQT8vtUQvM68kNSczVS8ls7hQzyU1LbE0pyQ4tSgzMSezKrUoJLW4RCU8I7EktSy1qKDCwf3f0aucTAwMFQUAemoaAg==");

        DefaultSerializer<Object> serializer = new DefaultSerializer<>(true);

        Buffer buffer = new Buffer(bytes);
        assertThatThrownBy(() -> serializer.deserialize(buffer.read()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("DefaultSerializerTest$Whatever")
                .hasCauseInstanceOf(ClassNotFoundException.class);

    }

}