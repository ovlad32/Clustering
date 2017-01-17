package com.rokittech;

import java.io.StringWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class DbMetadataToXML {

	private Connection connection;
	private boolean nullTags = true;
	private String schemaName;
	private String sequenceQuery = "select s.* from information_schema.sequences s where s.sequence_schema = ? ";
	private String viewQuery = "select s.* from information_schema.views s where s.table_schema = ? ";
	private String tableQuery = "select s.* from information_schema.tables s where s.table_type='BASE TABLE' and s.table_schema = ? order by table_name ";

	private void appendNullAtribute(Element element) {
		element.setAttribute("xsi:nil", "true");
	}

	private void appendXSIAtribute(Element element) {
		element.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
	}

	public List<DbItem> fetchAll() {
		List<DbItem> result = new ArrayList<>();

		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder;
		try {
			builder = dbf.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
			throw new RuntimeException(e);
		}
		DOMImplementation impl = builder.getDOMImplementation();
		result.addAll(fetchSimple(impl, "sequence", "sequence_name", sequenceQuery, null));
		result.addAll(fetchSimple(impl, "view", "table_name", viewQuery, null));

		result.addAll(fetchSimple(impl, "table", "table_name", tableQuery, Arrays.asList(new TableColumns(),new Constraints())));
		return result;
	}

	public List<DbItem> fetchSimple(DOMImplementation impl, String itemType, String itemNameColumnName, String query,
			List<AbstractDbItemProcessor> subProcessors) {
		List<DbItem> result = new ArrayList<>();
		try (PreparedStatement ps1 = connection.prepareCall(query)) {
			ps1.setString(1, schemaName);
			ResultSetMetaData rsm = null;
			int columnCount = 0;
			try (ResultSet rs = ps1.executeQuery()) {
				while (rs.next()) {
					Document doc = impl.createDocument(null, null, null);
					if (rsm == null) {
						rsm = rs.getMetaData();
						columnCount = rsm.getColumnCount();
					}
					Element row = doc.createElement("row");
					appendXSIAtribute(row);
					for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
						Element elem = null;
						Object value = rs.getObject(columnIndex);
						if (value == null) {
							if (nullTags) {
								elem = doc.createElement(rsm.getColumnName(columnIndex));
								appendNullAtribute(elem);
							}
						} else {
							elem = doc.createElement(rsm.getColumnName(columnIndex));
							elem.setTextContent(value.toString());
						}
						if (elem != null) {
							row.appendChild(elem);
						}
					}
					doc.appendChild(row);
					if (subProcessors != null) {
						for (AbstractDbItemProcessor proc : subProcessors) {
							proc.process(rs, row);
						}
					}
					result.add(new DbItem(rs.getString(itemNameColumnName), itemType, doc));
				}
			}

		} catch (SQLException e) {
			throw new RuntimeException("Fetching " + itemType + " info", e);
		}
		return result;
	}

	private void connect() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		Driver d = (Driver) Class.forName("org.postgresql.Driver").newInstance();
		Properties p = new Properties();
		p.put("user", "gpadmin");
		p.put("password", "pivotal");
		this.connection = d.connect("jdbc:postgresql://10.200.80.143:5432/postgres", p);
	}

	private void printResult(List<DbItem> items) throws TransformerException {
		TransformerFactory tf = TransformerFactory.newInstance();
		Transformer transformer = tf.newTransformer();
		transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
		transformer.setOutputProperty(OutputKeys.METHOD, "xml");
		// transformer.setOutputProperty(OutputKeys.ENCODING, "ISO-8859-1");
		for (DbItem item : items) {
			StringWriter sw = new StringWriter();
			StreamResult sr = new StreamResult(sw);
			DOMSource domSource = new DOMSource(item.getDomDocument());
			transformer.transform(domSource, sr);
			System.out.println(String.format("%s|%s|%s", item.getType(), item.getName(), sw.toString()));
		}
	}

	public static void main(String[] args) throws InstantiationException, IllegalAccessException,
			ClassNotFoundException, SQLException, TransformerException {

		DbMetadataToXML instance = new DbMetadataToXML();
		instance.connect();
		instance.schemaName = "cra";
		instance.printResult(instance.fetchAll());

	}

	abstract class InternalContextProcessor extends AbstractDbItemProcessor {
		@Override
		public boolean isNullPresent() {
			return DbMetadataToXML.this.nullTags;
		}

		@Override
		public List<AbstractDbItemProcessor> getSubProcessors() {
			return null;
		}
	}

	class TableColumns extends InternalContextProcessor {

		@Override
		public String getRootElementName() {
			return "columns";
		}

		@Override
		public String getQuery() {
			return "select c.* from information_schema.columns c where c.table_schema = ? and c.table_name = ?";
		}

		@Override
		public List<String> getQueryParameterParentColumns() {
			return Arrays.asList("table_schema", "table_name");
		}

	}

	class Constraints extends InternalContextProcessor {

		@Override
		public String getRootElementName() {
			return "constraints";
		}

		@Override
		public String getQuery() {
			return "select c.* from information_schema.table_constraints c where c.table_schema=? and c.table_name=?";
		}

		@Override
		public List<String> getQueryParameterParentColumns() {
			return Arrays.asList("table_schema", "table_name");
		}

		@Override
		public List<AbstractDbItemProcessor> getSubProcessors() {
			return Arrays.asList(new CheckConstraints(), 
					new PrimaryOrUniqueKeyColumnList (),
					new ForeignKeyConstraints());
		}
	}

	class CheckConstraints extends InternalContextProcessor {

		@Override
		public String getRootElementName() {
			return "check_constraint_details";
		}

		@Override
		public String getQuery() {
			return "select c.* from information_schema.check_constraints c where c.constraint_schema=? and c.constraint_name=?";
		}

		@Override
		public List<String> getQueryParameterParentColumns() {
			return Arrays.asList("constraint_schema", "constraint_name");
		}
	}

	class PrimaryOrUniqueKeyColumnList  extends InternalContextProcessor {
		@Override
		public String getRootElementName() {
			return "key_column_list";
		}

		@Override
		public String getQuery() {
			return "select c.* from information_schema.key_column_usage c "
					+ " inner join information_schema.table_constraints tc "
					+ "  on tc.constraint_schema = c.constraint_schema "
					+ " and tc.constraint_name = c.constraint_name "
					+ " and tc.constraint_type in ('PRIMARY KEY','UNIQUE') "
					+ " where c.constraint_schema = ?"
					+ "  and c.constraint_name = ? "
					+ " order by c.ordinal_position";
		}

		@Override
		public List<String> getQueryParameterParentColumns() {
			return Arrays.asList("constraint_schema","constraint_name");
		}
		
	}
	
	class ForeignKeyConstraints extends InternalContextProcessor {
		@Override
		public String getRootElementName() {
			return "foreign_key_constraint_details";
		}

		@Override
		public String getQuery() {
			return "select c.* from information_schema.referential_constraints c where c.constraint_schema=? and c.constraint_name=?";
		}

		@Override
		public List<String> getQueryParameterParentColumns() {
			return Arrays.asList("constraint_schema","constraint_name");
		}
		@Override
		public List<AbstractDbItemProcessor> getSubProcessors() {
			return Arrays.asList(new ReferencingColumnsList());
		}
	}
	
	class ReferencingColumnsList extends InternalContextProcessor {
		@Override
		public String getRootElementName() {
			return "referencing_column_list";
		}

		@Override
		public String getQuery() {
			return "select c.* from information_schema.key_column_usage c "
					+ " inner join information_schema.table_constraints tc "
					+ "  on tc.constraint_schema = c.constraint_schema "
					+ " and tc.constraint_name = c.constraint_name "
					+ " and tc.constraint_type ='FOREIGN KEY' "
					+ " where c.constraint_schema = ?"
					+ "  and c.constraint_name = ?"
					+ " order by c.ordinal_position";
		}

		@Override
		public List<String> getQueryParameterParentColumns() {
			return Arrays.asList("constraint_schema","constraint_name");
		}
	}
	
	
	
}
