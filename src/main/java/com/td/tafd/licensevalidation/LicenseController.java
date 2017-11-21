/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.td.tafd.licensevalidation;

import com.td.tma.licsvc.LicenseVO;
import com.td.tma.licsvc.LicenseValidator;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.commons.io.FileUtils;
import org.jasypt.util.text.BasicTextEncryptor;

/**
 *
 * @author mb255051
 */
public class LicenseController {

    private static LicenseController singleton;

    private LicenseVO licenseVO;
    private final String password; // for encryption and decryption
    private String filepath = "/resources/license.dat";

    public static LicenseController getInstance() {
        if (singleton == null) {
            singleton = new LicenseController();
        }

        return singleton;
    }

    public LicenseController() {
        licenseVO = new LicenseVO();
        password = "Rgd!s+f5lB3lSM0)4S|F!s57i#^ItYwz%elZ:5AQ?x)MT+K%uf864:0.EP9soKj]";
    }

    public boolean validateLicense(String userId, String licenseKey, String registrationKey) {
        try {
            licenseVO = new LicenseValidator().validateLicense(userId, licenseKey, registrationKey);

            if (licenseVO == null) {
                return false;
            }

            if (hasLicenseExpired()) {
                return false;
            }

            return isLicenseForTAF();

        } catch (Exception e) {
            return false;
        }
    }

    public boolean hasLicenseExpired() {
        if (licenseVO == null) {
            return true;
        }

        Date expiryDate = licenseVO.getExpiryDate();
        return expiryDate.before(new Date());
    }

    public boolean isLicenseForTAF() {
        if (licenseVO == null) {
            return false;
        }

        return licenseVO.getSupportedTmaModules().contains("Testing Automation Framework");
    }

    /**
     * Reads contents of license file, decrypts them, and populates respective
     * variables.
     * @return 
     */
    public LicenseVO readFromLicenseFile() {
        String licenseInfo = null;
        String encryptedLicenseInfo = null;
        Map<String, String> licenseInfoMap = new HashMap<String, String>();

        // Read file
        File licenseFile = new File(filepath);

        try {
            encryptedLicenseInfo = FileUtils.readFileToString(licenseFile);
            licenseInfo = decryptLicenseInfo(encryptedLicenseInfo);
            licenseInfoMap = parseLicenseInfo(licenseInfo);
            licenseVO.setUserId(licenseInfoMap.get("userId"));
            licenseVO.setLicenseKey(licenseInfoMap.get("licenseKey"));
            licenseVO.setRegistrationKey(licenseInfoMap.get("regKey"));
        } catch (Exception e) {
            
        }
        return licenseVO;
    }

    public void writeToLicenseFile() {
        StringBuilder sb = new StringBuilder();
        sb.append("userId");
        sb.append("`");
        sb.append(licenseVO.getUserId());
        sb.append("`");

        sb.append("licenseKey");
        sb.append("`");
        sb.append(licenseVO.getLicenseKey());
        sb.append("`");

        sb.append("regKey");
        sb.append("`");
        sb.append(licenseVO.getRegistrationKey());
        sb.append("`");

        String encrypted = encryptLicenseInfo(sb.toString());
        try {
            FileUtils.writeStringToFile(new File(filepath), encrypted);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String decryptLicenseInfo(String cipher) {
        String plainText;

        BasicTextEncryptor textEncryptor = new BasicTextEncryptor();
        textEncryptor.setPassword(password);

        plainText = textEncryptor.decrypt(cipher);

        return plainText;

    }

    public String encryptLicenseInfo(String plainText) {
        String cipher;

        BasicTextEncryptor textEncryptor = new BasicTextEncryptor();
        textEncryptor.setPassword(password);

        cipher = textEncryptor.encrypt(plainText);

        return cipher;
    }

    public Map<String, String> parseLicenseInfo(String licenseInfo) {
        Map<String, String> licenseInfoMap = new HashMap<String, String>();

        StringTokenizer st = new StringTokenizer(licenseInfo, "`");
        while (st.hasMoreTokens()) {
            String key = st.nextToken().replaceAll("[\n\r]", "");;
            String val = st.nextToken();
            licenseInfoMap.put(key, val);
        }

        return licenseInfoMap;
    }

    public LicenseVO getLicenseVO() {
        return licenseVO;
    }

    public boolean validateButtonAction(String userId, String licenseKey, String registrationKey) {
    	LicenseView licenseView = new LicenseView();
        licenseVO = new LicenseValidator().validateLicense(
                userId,
                licenseKey,
                registrationKey);
        if (licenseVO != null) {
            System.out.println("userId: " + licenseVO.getUserId());
            Date expiryDate = licenseVO.getExpiryDate();
            if (expiryDate.before(new Date())) {
                licenseView.updateStatusMessageOnFailure("The license you obtained has expired. Please obtain a new license.");
                return false;
            } else if (!licenseVO.getSupportedTmaModules().contains("Testing Automation Framework")) {
            	licenseView.updateStatusMessageOnFailure("The license you obtained is not for Testing Automation Framework.");
            	return false;
            } else {  // Success!
                writeToLicenseFile();
                licenseView.updateStatusMessageOnSuccess("License Validated!!");
				licenseView.showValidateButton(false);
				return true;
            }

        } else {
        	licenseView.updateStatusMessageOnFailure("Could not validate your license information. Please re-check.");
        	return false;
        }
    }
}
