package com.rokittech;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public abstract class AbstractDbItemProcessor {
	abstract public String getRootElementName();

	abstract public String getQuery();

	abstract public boolean isTagForNull();

	abstract public List<String> getQueryParameterParentColumns();

	abstract public List<AbstractDbItemProcessor> getSubProcessors();

	public void process(ResultSet parentResultSet, Element parentRowElement) {
		List<AbstractDbItemProcessor> subProcessors = getSubProcessors();
		Document doc = parentRowElement.getOwnerDocument();
		NodeList nodeList =  parentRowElement.getElementsByTagName(getRootElementName());
		Node root = null;
		if (nodeList.getLength()>0)
			root = nodeList.item(0);
		else 
		 	root = doc.createElement(getRootElementName());
	
		

		try (PreparedStatement ps1 = parentResultSet.getStatement().getConnection().prepareCall(getQuery())) {
			int indexParameter = 0;

			for (String parameterColumnName : getQueryParameterParentColumns()) {
				ps1.setObject(++indexParameter, parentResultSet.getObject(parameterColumnName));
			}
			ResultSetMetaData rsm = null;
			int columnCount = 0;
			try (ResultSet rs = ps1.executeQuery()) {
				while (rs.next()) {
					if (rsm == null) {
						parentRowElement.appendChild(root);
						rsm = rs.getMetaData();
						columnCount = rsm.getColumnCount();
					}
					Element row = doc.createElement("row");
					row.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
					for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
						Element elem = null;
						Object value = rs.getObject(columnIndex);
						if (value == null) {
							if (isTagForNull()) {
								elem = doc.createElement(rsm.getColumnName(columnIndex));
								elem.setAttribute("xsi:nil", "true");
							}
						} else {
							elem = doc.createElement(rsm.getColumnName(columnIndex));
							elem.setTextContent(value.toString());
						}
						if (elem != null) {
							row.appendChild(elem);
						}
					}
					root.appendChild(row);
					if (subProcessors != null) {
						for (AbstractDbItemProcessor dp : subProcessors) {
							dp.process(rs, row);
						}
					}
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException("create subquery for " + getRootElementName(), e);
		}
	}
}
