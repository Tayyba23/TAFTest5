/**
 * 
 */
package com.td.tafd.validation;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import com.td.tafd.validation.Validator;

/**
 * @author kt186036
 *
 */
public class ValidatorTest {

	@Test
	public void testValidateIpPass() {
		Validator validator = Validator.getInstance();
		assertEquals("153.65.144.102 should be a valid IP", true, validator.validateIP("153.65.144.102"));
	}
	
	@Test
	public void testValidateIpFail() {
		Validator validator = Validator.getInstance();
		assertEquals("500.1.2.3 should be an invalid IP", false, validator.validateIP("500.1.2.3"));
		assertEquals("abc123 should be an invalid IP", false, validator.validateIP("abc123"));
	}
	
	public static void main(String[] args) 
	{
		Result result = JUnitCore.runClasses(ValidatorTest.class);
		for (Failure failure : result.getFailures()) {
			System.out.println(failure.toString());
		}
	}
}
