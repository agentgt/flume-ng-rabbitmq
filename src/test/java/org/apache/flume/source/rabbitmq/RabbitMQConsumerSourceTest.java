package org.apache.flume.source.rabbitmq;

import static org.junit.Assert.*;

import org.junit.Test;

import com.google.common.base.Charsets;

public class RabbitMQConsumerSourceTest {

	@Test
	public void test() {
		byte[] input = "0123456789".getBytes(Charsets.UTF_8);
		assertEquals(input.length, 10);
		
		byte[] b = RabbitMQConsumerSource.maybeTruncate("text/plain", input, 8);
		assertEquals(b.length, 8);
		assertEquals("01234567", new String(b));
	}

}
