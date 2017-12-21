import java.io.ByteArrayInputStream;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

public class SAXParser extends DefaultHandler {
	
	// "stn", "date", "time", "dewp", "stp", "slp", "visib", "wdsp", "prcp", "sndp", "frshtt", "cldc", "wnddir"
	
	private boolean stn_flag, date_flag, time_flag = false;
	private String stn, date, time;
	private Database db;
	

	public void parse(String input) {
      try {
		db = new Database();
		XMLReader p = XMLReaderFactory.createXMLReader();
		p.setContentHandler(this);
		p.parse( new InputSource( new ByteArrayInputStream(input.getBytes()) ) );
      } catch (Exception ex) {
         ex.printStackTrace();
      }
      
	}
	
	
	@Override
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
		stn_flag = qName.equals("STN");
		date_flag = qName.equals("DATE");
		time_flag = qName.equals("TIME");
	}
	
	@Override
	public void characters(char[] ch, int start, int length) throws SAXException {
		if( stn_flag )
			stn = (new String(ch, start, length));
			stn_flag = false;
		if( date_flag )
			date = (new String(ch, start, length));
			date_flag = false;
		if( time_flag )
			time = (new String(ch, start, length));
			time_flag = false;
	}
	
	@Override
	public void endElement(String uri, String localName, String qName) throws SAXException {
		if ( localName.equals( "MEASUREMENT" ) ) {
			String query = "INSERT INTO measurement (`stn`,`date`,`time`) VALUES (" + stn + ",'" + date + "','" + time + "')";
			System.out.println(query);
			db.insert(query);
		}
	}
	
}
