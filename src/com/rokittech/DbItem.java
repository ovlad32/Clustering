package com.rokittech;

import org.w3c.dom.Document;

public class DbItem {

	private String name;
	private String type;
	private Document domDocument;
	
	public DbItem(String name, String type, Document domDocument) {
		super();
		this.name = name;
		this.type = type;
		this.domDocument = domDocument;
	}

	public String getName() {
		return name;
	}

	public String getType() {
		return type;
	}

	public Document getDomDocument() {
		return domDocument;
	}
	
	
	
	
}

