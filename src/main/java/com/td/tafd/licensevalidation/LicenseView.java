/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.td.tafd.licensevalidation;

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.GroupLayout;
import javax.swing.ImageIcon;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.LayoutStyle;
import javax.swing.text.AbstractDocument;

import com.td.tafd.util.DocumentSizeFilter;

/**
 *
 * @author mb255051
 */
public class LicenseView extends JFrame {
	
	private static final long serialVersionUID = 3614671712250396342L;
	
	private JLabel msgLabel;
	private ImageIcon msgIconNo;
	private ImageIcon msgIconYes;
//	private ImageIcon msgIconInfo;
	private ImageIcon msgIconSpacer;

	private JLabel userIdLabel;
	private JTextField userIdTextField;

	private JLabel licenseKeyLabel;
	private JTextArea licenseKey;
	private JScrollPane scrollLicenseKey;

	private JLabel regKeyLabel;
	private JTextArea regKey;
	private JScrollPane scrollRegKey;

	private JButton validateButton;
	private JButton updateButton;
//	private JButton detailsButton;
	
	private boolean validated = false;

	private void defineComponents() {
		
		msgIconNo = new ImageIcon("/resources/error.png");
		msgIconYes = new ImageIcon("/resources/success.png");
//		msgIconInfo = new ImageIcon("/resources/info.png");
		msgIconSpacer = new ImageIcon("/resources/spacer.png");

		msgLabel = new JLabel("   ", msgIconSpacer, JLabel.CENTER);
		// msgLabel.setFont(fontVerdana11);

		userIdLabel = new JLabel("User ID");
		// userIdLabel.setFont(fontVerdana11);
		userIdLabel.setForeground(Color.red);
		userIdTextField = new JTextField(24);
		// userIdTextField.setFont(fontVerdana11);

		licenseKeyLabel = new JLabel("License Key");
		// licenseKeyLabel.setFont(fontVerdana11);
		licenseKeyLabel.setForeground(Color.red);
		licenseKey = new JTextArea(12, 64);
		licenseKey.setLineWrap(true);
		AbstractDocument pdLicKey = (AbstractDocument) licenseKey.getDocument();
		pdLicKey.setDocumentFilter(new DocumentSizeFilter(2500));// Limit the
																	// registration
																	// key to
																	// 2500
																	// characters
		scrollLicenseKey = new JScrollPane(licenseKey);

		regKeyLabel = new JLabel("Registration Key");
		// regKeyLabel.setFont(fontVerdana11);
		regKeyLabel.setForeground(Color.red);
		regKey = new JTextArea();
		regKey.setLineWrap(true);
		AbstractDocument pdRegKey = (AbstractDocument) regKey.getDocument();
		pdRegKey.setDocumentFilter(new DocumentSizeFilter(1000));// Limit the
																	// registration
																	// key to
																	// 1000
																	// characters
		scrollRegKey = new JScrollPane(regKey);

		validateButton = new JButton("Validate");
		// validateButton.setFont(fontVerdana11);

		updateButton = new JButton("Update");
		// updateButton.setFont(fontVerdana11);

//		detailsButton = new JButton("Details");
		// detailsButton.setFont(fontVerdana11);

		// Add ActionListener for validateButton
		validateButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				validateControlsData();
				validated = LicenseController.getInstance().validateButtonAction(userIdTextField.getText(), licenseKey.getText(), regKey.getText());
			}
		});

		// Add ActionListener for updateButton
		updateButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				validateControlsData();
				validated = LicenseController.getInstance().validateButtonAction(userIdTextField.getText(), licenseKey.getText(), regKey.getText());
			}
		});

		// Add ActionListener for detailsButton
//		detailsButton.addActionListener(new ActionListener() {
//			public void actionPerformed(ActionEvent e) {
//				try {
////					LicenseDetailsView detailsForm = new LicenseDetailsView();
//				} catch (Exception e1) {
//					// TODO Auto-generated catch block
//					e1.printStackTrace();
//				}
//			}
//		});
	}

	private void layoutComponents() {
		JPanel pane = (JPanel) getContentPane();
		
		GroupLayout layout = new GroupLayout(pane);
		pane.setLayout(layout);
		layout.setAutoCreateGaps(true);
		layout.setAutoCreateContainerGaps(true);
		
		layout.setHorizontalGroup(
				layout.createParallelGroup()
					.addComponent(msgLabel)
					.addGroup(layout.createSequentialGroup()
						.addGroup(layout.createParallelGroup(GroupLayout.Alignment.TRAILING)
							.addComponent(userIdLabel)
							.addComponent(licenseKeyLabel)
							.addComponent(regKeyLabel))
						.addGroup(layout.createParallelGroup()
							.addComponent(userIdTextField, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
							.addComponent(scrollLicenseKey)
							.addComponent(scrollRegKey)
							.addGroup(layout.createSequentialGroup()
								.addComponent(validateButton)
								.addComponent(updateButton))))
			);
		
		layout.setVerticalGroup(
				layout.createSequentialGroup()
					.addComponent(msgLabel)
					.addPreferredGap(LayoutStyle.ComponentPlacement.UNRELATED, 24, 24/*, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE*/)
					.addGroup(layout.createParallelGroup(GroupLayout.Alignment.BASELINE)
						.addComponent(userIdLabel)
						.addComponent(userIdTextField, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addGroup(layout.createParallelGroup()
						.addComponent(licenseKeyLabel)
						.addComponent(scrollLicenseKey,150,150,150))// GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
					.addGroup(layout.createParallelGroup()
						.addComponent(regKeyLabel)
						.addGroup(layout.createSequentialGroup()
							.addComponent(scrollRegKey,130,130,130)// GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
							.addGroup(layout.createParallelGroup()
								.addComponent(validateButton)
								.addComponent(updateButton))))
			);
		pack();
	}

	/**
	 * Creates new form LicenseDetails
	 */
	public LicenseView() {
		defineComponents();
		layoutComponents();
//		this.setVisible(true);
//		initComponents();

	}

	public void updateStatusMessageOnFailure(String msg) {
		msgLabel.setText(msg);
		msgLabel.setIcon(msgIconNo);
	}

	public void updateStatusMessageOnSuccess(String msg) {
		msgLabel.setText(msg);
		msgLabel.setIcon(msgIconYes);
	}

	public boolean validateControlsData() {
		if (userIdTextField.getText().trim().isEmpty()) {
			updateStatusMessageOnFailure("Please enter the User ID.");
			return false;
		}

		if (licenseKey.getText().trim().isEmpty()) {
			updateStatusMessageOnFailure("Please enter the License Key.");
			return false;
		}

		if (regKey.getText().trim().isEmpty()) {
			updateStatusMessageOnFailure("Please enter the Registration Key.");
			return false;
		}

		return true;
	}
	
	public void showValidateButton(boolean what) {
		validateButton.setVisible(what);
		updateButton.setVisible(!what);
//		detailsButton.setVisible(!what);
	}
	
	public void setValidated(boolean validated) {
		this.validated = validated;
	}
	
	public boolean isValidated() {
		return validated;
	}
}
