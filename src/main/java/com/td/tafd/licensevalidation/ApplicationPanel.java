/**
 * 
 */
package com.td.tafd.licensevalidation;

import javax.swing.JPanel;

/**
 * @author mb255051
 *
 */
public abstract class ApplicationPanel extends JPanel {
	private static final long serialVersionUID = 1146058011970369692L;
	
	protected int panelHeight;
    protected int panelWidth;

//    public abstract String getPanelName() throws Exception;
    protected abstract void defineComponents() throws Exception;
    protected abstract void layoutComponents() throws Exception;
    public abstract void setMessage(String msg, boolean errorFlag);
}
