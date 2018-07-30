import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Scanner;
import java.util.Stack;

/**
 * @author Shubham Raosaheb Kharde
 * @version 1.0
 */
public class DavisBaseLite {

	static String copyright = "©2018 Shubham Raosaheb Kharde";
	private static final String ERROR_MESSAGE = "You have an error in your syntax. Check manual using 'help' command";
	static boolean isExit = false;
	static long pageSize = 512;
	static String prompt = "davisql> ";
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	static String version = "v1.0";

	/**
	 * ***********************************************************************
	 * Static method definitions
	 */

	public static String calculateDatatype(int serialcode) {
		switch (serialcode) {
		case 0:
			return "null tinyint";
		case 1:
			return "null smallint";
		case 2:
			return "null int"; // null real
		case 3:
			return "null double"; // null datetime, null date, null bigint
		case 4:
			return "tinyint";
		case 5:
			return "smallint";
		case 6:
			return "int";
		case 7:
			return "bigint";
		case 8:
			return "real";
		case 9:
			return "double";
		case 10:
			return "datetime";
		case 11:
			return "date";
		case 12:
			return "text";

		default:
			return "text";

		}
	}

	public static String calculateMethod(String type) {
		switch (type) {
		case "null tinyint":
		case "tinyint":
			return "byte";
		case "null smallint":
		case "smallint":
			return "short";
		case "null int":
		case "null real":
		case "int":
			return "int";
		case "null double":
		case "null datetime":
		case "null date":
		case "null bigint":
		case "bigint":
		case "datetime":
		case "date":
			return "long";
		case "real":
			return "float";
		case "double":
			return "double";
		case "text":
			return "line";

		default:
			return "line";

		}
	}

	public static int[] calculateSerialCodeAndSize(String type) {
		switch (type) {
		case "null tinyint":
			return new int[] { 0, 1 };
		case "null smallint":
			return new int[] { 1, 2 };
		case "null int":
		case "null real":
			return new int[] { 2, 4 };
		case "null double":
		case "null datetime":
		case "null date":
		case "null bigint":
			return new int[] { 3, 8 };
		case "tinyint":
			return new int[] { 4, 1 };
		case "smallint":
			return new int[] { 5, 2 };
		case "int":
			return new int[] { 6, 4 };
		case "bigint":
			return new int[] { 7, 8 };
		case "real":
			return new int[] { 8, 4 };
		case "double":
			return new int[] { 9, 8 };
		case "datetime":
			return new int[] { 10, 8 };
		case "date":
			return new int[] { 11, 8 };
		case "text":
			return new int[] { 12, 0 };

		default:
			return new int[] { 12, 0 };

		}
	}

	public static long createPage(RandomAccessFile file, boolean isLeaf) {
		try {
			file.setLength(file.length() + pageSize);

			file.seek(file.length() - pageSize);
			if (isLeaf) {
				file.write(0x0D);
			} else {
				file.write(0x05);
			}
			file.write(0x00);
			file.writeShort((int) file.length());
			file.write(0xff);
			file.write(0xff);
			file.write(0xff);
			file.write(0xff);

			return (file.length() / pageSize) - 1;
		} catch (Exception e) {
			System.out.println(ERROR_MESSAGE);
		}
		return 0;
	}

	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}

	private static void droptable(String table_name) {
		try {
			RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			int seek = 0;
			int right_child_pointer = 0;
			int page = 0;
			int records = 0;
			boolean dropped = false;
			while (right_child_pointer != -1) {
				seek = (int) (page * pageSize);
				davisbaseTablesCatalog.seek(seek + 4);
				right_child_pointer = davisbaseTablesCatalog.readInt();
				page = right_child_pointer;
				davisbaseTablesCatalog.seek(seek + 1);
				records = davisbaseTablesCatalog.readByte();
				for (int i = 0; i < records; i++) {
					int posToRead = seek + 8 + (2 * i);
					davisbaseTablesCatalog.seek(posToRead);
					posToRead = davisbaseTablesCatalog.readShort();

					davisbaseTablesCatalog.seek(posToRead);
					int payload_size = davisbaseTablesCatalog.readShort();

					davisbaseTablesCatalog.seek(posToRead + 6);
					int columns = davisbaseTablesCatalog.readByte();

					davisbaseTablesCatalog.seek(posToRead + 6 + 1 + columns);
					String table_name_found = davisbaseTablesCatalog.readLine();

					davisbaseTablesCatalog.seek((posToRead + payload_size + 6) - 1);
					int is_active = davisbaseTablesCatalog.readByte();

					if (table_name.equals(table_name_found) && (is_active == 1)) {
						davisbaseTablesCatalog.seek((posToRead + payload_size + 6) - 1);
						davisbaseTablesCatalog.writeByte(0);
						dropped = true;
					}
				}
			}

			davisbaseTablesCatalog.close();

			RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			seek = 0;
			right_child_pointer = 0;
			page = 0;
			records = 0;
			dropped = false;
			while (right_child_pointer != -1) {
				seek = (int) (page * pageSize);
				davisbaseColumnsCatalog.seek(seek + 4);
				right_child_pointer = davisbaseColumnsCatalog.readInt();
				page = right_child_pointer;
				davisbaseColumnsCatalog.seek(seek + 1);
				records = davisbaseColumnsCatalog.readByte();
				for (int i = 0; i < records; i++) {
					int posToRead = seek + 8 + (2 * i);
					davisbaseColumnsCatalog.seek(posToRead);
					posToRead = davisbaseColumnsCatalog.readShort();

					davisbaseColumnsCatalog.seek(posToRead + 6);
					int columns = davisbaseColumnsCatalog.readByte();

					davisbaseColumnsCatalog.seek(posToRead);
					int payload_size = davisbaseColumnsCatalog.readShort();

					davisbaseColumnsCatalog.seek(posToRead + 6 + 1 + columns);
					String table_name_found = davisbaseColumnsCatalog.readLine();

					davisbaseColumnsCatalog.seek((posToRead + payload_size + 6) - 1);
					int is_active = davisbaseColumnsCatalog.readByte();

					if (table_name.equals(table_name_found) && (is_active == 1)) {
						davisbaseColumnsCatalog.seek((posToRead + payload_size + 6) - 1);
						davisbaseColumnsCatalog.writeByte(0);
						dropped = true;
					}
				}
			}

			davisbaseColumnsCatalog.close();
			if (!dropped) {
				System.out.println("Table '" + table_name + "' doesnt exist");
			} else {
				System.out.println("Table '" + table_name + "' dropped successfully");
			}

		} catch (Exception e) {
			System.out.println(ERROR_MESSAGE);
		}

	}

	/**
	 * Stub method for dropping tables
	 *
	 * @param dropTableString
	 *            is a String of the user input
	 */
	public static void dropTable(String dropTableString) {
		ArrayList<String> dropTokens = new ArrayList<String>(Arrays.asList(dropTableString.split(" ")));
		try {
			String table_name = dropTokens.get(2);
			if (table_name.equals("davisbase_tables") || table_name.equals("davisbase_columns")) {
				System.out.println("Cannot drop database catalog (meta-data) table");
				return;
			}
			if (!dropTokens.get(1).equals("table")) {
				System.out.println(ERROR_MESSAGE);
				return;
			}
			droptable(table_name);
		} catch (Exception e) {
			System.out.println(ERROR_MESSAGE);
		}

	}

	public static void getColumnMetaData(String table_name, ArrayList<String> attribute_names,
			ArrayList<String> data_types, ArrayList<String> constraints) {
		try {
			RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "r");
			int seek = 0;
			int right_child_pointer = 0;
			int page = 0;
			int records = 0;
			boolean skipRowId = true;
			while (right_child_pointer != -1) {
				seek = (int) (page * pageSize);
				davisbaseColumnsCatalog.seek(seek + 4);
				right_child_pointer = davisbaseColumnsCatalog.readInt();
				page = right_child_pointer;
				davisbaseColumnsCatalog.seek(seek + 1);
				records = davisbaseColumnsCatalog.readByte();
				for (int i = 0; i < records; i++) {
					int posToRead = seek + 8 + (2 * i);
					davisbaseColumnsCatalog.seek(posToRead);
					posToRead = davisbaseColumnsCatalog.readShort();

					davisbaseColumnsCatalog.seek(posToRead + 6);
					int columns = davisbaseColumnsCatalog.readByte();

					davisbaseColumnsCatalog.seek(posToRead);
					int payload_size = davisbaseColumnsCatalog.readShort();

					int code = davisbaseColumnsCatalog.readByte();
					String type = calculateDatatype(code);
					int[] codeAndSize = calculateSerialCodeAndSize(type);
					int bytesRead = code - codeAndSize[0];

					davisbaseColumnsCatalog.seek(posToRead + 6 + 1 + columns);
					String table_name_found = davisbaseColumnsCatalog.readLine();

					davisbaseColumnsCatalog.seek((posToRead + payload_size + 6) - 1);
					int is_active = davisbaseColumnsCatalog.readByte();

					if (table_name.equals(table_name_found) && (is_active == 1)) {
						if (!skipRowId) {
							for (int k = 0; k < (columns - 1); k++) {
								davisbaseColumnsCatalog.seek(posToRead + 6 + 1 + k);
								code = davisbaseColumnsCatalog.readByte();
								type = calculateDatatype(code);
								codeAndSize = calculateSerialCodeAndSize(type);

								davisbaseColumnsCatalog.seek(posToRead + 6 + 1 + columns + bytesRead);
								if (k == 1) {
									attribute_names.add(davisbaseColumnsCatalog.readLine());
								}
								if (k == 2) {
									data_types.add(davisbaseColumnsCatalog.readLine());
								}
								if (k == 4) {
									constraints.add(davisbaseColumnsCatalog.readLine());
								}

								if (type.equals("text")) {
									bytesRead = (bytesRead + code) - codeAndSize[0];
								} else {
									bytesRead = bytesRead + codeAndSize[1];
								}
							}
						}
						skipRowId = false;
					}
				}
			}

			davisbaseColumnsCatalog.close();
		} catch (Exception e) {
			System.out.println(ERROR_MESSAGE);
		}

	}

	public static String getCopyright() {
		return copyright;
	}

	public static int getfromMetaTable(String table_name, boolean getRootPage, boolean getRecordCount) {
		int value = 0;
		if (getRootPage) {
			value = -1;
		}
		if (getRecordCount) {
			value = 0;
		}
		try {
			RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "r");
			int seek = 0;
			int right_child_pointer = 0;
			int page = 0;
			int records = 0;
			while (right_child_pointer != -1) {
				seek = (int) (page * pageSize);
				davisbaseTablesCatalog.seek(seek + 4);
				right_child_pointer = davisbaseTablesCatalog.readInt();
				page = right_child_pointer;
				davisbaseTablesCatalog.seek(seek + 1);
				records = davisbaseTablesCatalog.readByte();
				for (int i = 0; i < records; i++) {
					int posToRead = seek + 8 + (2 * i);
					davisbaseTablesCatalog.seek(posToRead);
					posToRead = davisbaseTablesCatalog.readShort();

					davisbaseTablesCatalog.seek(posToRead);
					int payload_size = davisbaseTablesCatalog.readShort();

					davisbaseTablesCatalog.seek(posToRead + 6);
					int columns = davisbaseTablesCatalog.readByte();

					davisbaseTablesCatalog.seek(posToRead + 6 + 1 + columns);
					String table_name_found = davisbaseTablesCatalog.readLine();

					if (table_name.equals(table_name_found)) {
						davisbaseTablesCatalog.seek((posToRead + payload_size + 6) - 1);
						int is_active = davisbaseTablesCatalog.readByte();
						if (is_active == 1) {
							if (getRootPage) {
								davisbaseTablesCatalog.seek((posToRead + payload_size + 6) - 3);
								value = davisbaseTablesCatalog.readShort();
							}
							if (getRecordCount) {
								davisbaseTablesCatalog.seek((posToRead + payload_size + 6) - 7);
								value = davisbaseTablesCatalog.readInt();
							}
							break;
						}
					}
				}
			}
			davisbaseTablesCatalog.close();
		} catch (Exception e) {
			System.out.println(ERROR_MESSAGE);
		}

		return value;
	}

	/** return the DavisBase version */
	public static String getVersion() {
		return version;
	}

	/**
	 * Help: Display supported commands
	 */
	public static void help() {
		System.out.println(line("*", 80));
		System.out.println("SUPPORTED COMMANDS\n");
		System.out.println("All commands below are case insensitive\n");
		System.out.println("SHOW TABLES;");
		System.out.println("\tDisplay the names of all tables.\n");
		System.out.println(
				"CREATE TABLE <table_name> ( rowid INT PRIMARY KEY , <column_name> <data_type> [ NOT NULL ] );");
		System.out.println("\tCreate a table schema information for a new table.");
		System.out.println("\t'rowid' must be explicitly specified in the given format.\n");
		System.out.println("INSERT INTO <table_name> [(column_list)] VALUES ( value_list );");
		System.out.println("\tInserts a single record into a table.");
		System.out.println("\t'rowid' should not be supplied as it is generated in the background.\n");
		System.out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
		System.out.println("\tDisplay table records whose optional <condition> is <column_name> = <value>.\n");
		System.out.println("DROP TABLE <table_name>;");
		System.out.println("\tRemove table data (i.e. all records) and its schema.\n");
		System.out.println("UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];");
		System.out.println("\tModify records data whose optional <condition> is <column_name> = <value>.\n");
		System.out.println("VERSION;");
		System.out.println("\tDisplay the program version.\n");
		System.out.println("HELP;");
		System.out.println("\tDisplay this help information.\n");
		System.out.println("EXIT;");
		System.out.println("\tExit the program.\n");
		System.out.println(line("*", 80));
	}

	/**
	 * This static method creates the DavisBase data storage container and then
	 * initializes two .tbl files to implement the two system tables,
	 * davisbase_tables and davisbase_columns
	 */
	static void initializeDataStore() {

		/** Create data directory at the current OS location to hold */

		try {
			File dataDir = new File("data");
			dataDir.mkdir();

			File catalogDir = new File("data/catalog");
			catalogDir.mkdir();

			File userDir = new File("data/user_data");
			userDir.mkdir();

		} catch (SecurityException se) {
			System.out.println("Unable to create data container directory");
		}

		/** Create davisbase_tables system catalog */
		try {
			RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			if (davisbaseTablesCatalog.length() == 0) {
				createPage(davisbaseTablesCatalog, true);
			}

			davisbaseTablesCatalog.close();
		} catch (Exception e) {
			System.out.println("Unable to create meta-data files");
		}

		/** Create davisbase_columns systems catalog */
		try {
			RandomAccessFile davisbaseColumnsCatalog = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			if (davisbaseColumnsCatalog.length() == 0) {
				createPage(davisbaseColumnsCatalog, true);
			}
			davisbaseColumnsCatalog.close();

			int root_page = getfromMetaTable("davisbase_tables", true, false);

			if (root_page == -1) {

				insertIntoMetaTable("davisbase_tables", 0, (short) 0, (byte) 1);
				insertIntoMetaTable("davisbase_columns", 0, (short) 0, (byte) 1);

				ArrayList<String> table_names = new ArrayList<String>();
				table_names.add("davisbase_tables");
				table_names.add("davisbase_tables");
				table_names.add("davisbase_tables");
				table_names.add("davisbase_tables");
				table_names.add("davisbase_tables");

				table_names.add("davisbase_columns");
				table_names.add("davisbase_columns");
				table_names.add("davisbase_columns");
				table_names.add("davisbase_columns");
				table_names.add("davisbase_columns");
				table_names.add("davisbase_columns");
				table_names.add("davisbase_columns");

				ArrayList<String> attribute_names = new ArrayList<String>();
				attribute_names.add("rowid");
				attribute_names.add("table_name");
				attribute_names.add("record_count");
				attribute_names.add("root_page");
				attribute_names.add("is_active");

				attribute_names.add("rowid");
				attribute_names.add("table_name");
				attribute_names.add("column_name");
				attribute_names.add("data_type");
				attribute_names.add("ordinal_position");
				attribute_names.add("is_nullable");
				attribute_names.add("is_active");

				ArrayList<String> data_types = new ArrayList<String>();
				data_types.add("int");
				data_types.add("text");
				data_types.add("int");
				data_types.add("smallint");
				data_types.add("tinyint");

				data_types.add("int");
				data_types.add("text");
				data_types.add("text");
				data_types.add("text");
				data_types.add("tinyint");
				data_types.add("text");
				data_types.add("tinyint");

				ArrayList<String> ordinals = new ArrayList<String>();
				ordinals.add("1");
				ordinals.add("2");
				ordinals.add("3");
				ordinals.add("4");
				ordinals.add("5");

				ordinals.add("1");
				ordinals.add("2");
				ordinals.add("3");
				ordinals.add("4");
				ordinals.add("5");
				ordinals.add("6");
				ordinals.add("7");

				ArrayList<String> constraints = new ArrayList<String>();
				constraints.add("NO");
				constraints.add("NO");
				constraints.add("NO");
				constraints.add("NO");
				constraints.add("NO");

				constraints.add("NO");
				constraints.add("NO");
				constraints.add("NO");
				constraints.add("NO");
				constraints.add("NO");
				constraints.add("NO");
				constraints.add("NO");

				ArrayList<String> is_active = new ArrayList<String>();
				is_active.add("1");
				is_active.add("1");
				is_active.add("1");
				is_active.add("1");
				is_active.add("1");

				is_active.add("1");
				is_active.add("1");
				is_active.add("1");
				is_active.add("1");
				is_active.add("1");
				is_active.add("1");
				is_active.add("1");

				ArrayList<String> types = new ArrayList<String>();
				types.add("text");
				types.add("text");
				types.add("text");
				types.add("tinyint");
				types.add("text");
				types.add("tinyint");

				for (int k = 0; k < 12; k++) {
					root_page = getfromMetaTable("davisbase_columns", true, false);
					ArrayList<String> attributes = new ArrayList<String>();
					attributes.add(table_names.get(k));
					attributes.add(attribute_names.get(k));
					attributes.add(data_types.get(k));
					attributes.add(ordinals.get(k));
					attributes.add(constraints.get(k));
					attributes.add(is_active.get(k));

					insertIntoBtree("davisbase_columns", "data/catalog/davisbase_columns.tbl", root_page, attributes,
							types, attributes);
				}
			}
		} catch (Exception e) {
			System.out.println("Unable to create meta-data files");
		}
	}

	private static void insertIntoBtree(String table_name, String table_file_name, int root_page,
			ArrayList<String> attribute_names, ArrayList<String> data_types, ArrayList<String> entered_values) {
		try {
			RandomAccessFile tableFile = new RandomAccessFile(table_file_name, "rw");

			tableFile.seek(root_page * pageSize);
			Stack<Long> interiorPages = new Stack<Long>();
			while (tableFile.readByte() != 13) {
				interiorPages.add(root_page * pageSize);
				tableFile.seek((root_page * pageSize) + 4);
				root_page = tableFile.readInt();
				tableFile.seek(root_page * pageSize);
			}

			int original_page_no = root_page;
			long seek_index = original_page_no * pageSize;

			int rowid = getfromMetaTable(table_name, false, true) + 1;

			tableFile.seek(seek_index + 1);
			int no_of_records = tableFile.readByte() + 1;

			tableFile.seek(seek_index + 2);
			int lastWritePos = tableFile.readShort();

			int columns = attribute_names.size();

			ArrayList<Integer> codes = new ArrayList<Integer>();
			ArrayList<Integer> sizes = new ArrayList<Integer>();

			int payload_size = 1 + columns;
			for (int i = 0; i < columns; i++) {
				if (data_types.get(i).equals("text")) {
					entered_values.set(i, (entered_values.get(i) + "\r"));
					payload_size += entered_values.get(i).length();
					sizes.add(i, entered_values.get(i).length());
					codes.add(i, calculateSerialCodeAndSize("text")[0] + entered_values.get(i).length());
				} else {
					int[] codeAndSize;
					if (entered_values.get(i).equals("null")) {
						codeAndSize = calculateSerialCodeAndSize("null " + data_types.get(i));
					} else {
						codeAndSize = calculateSerialCodeAndSize(data_types.get(i));
					}

					codes.add(i, codeAndSize[0]);
					sizes.add(i, codeAndSize[1]);
					payload_size += codeAndSize[1];
				}
			}

			int header_size = 6;
			int record_size = header_size + payload_size;

			int posToWrite = lastWritePos - record_size;

			int lastPosOfPointers = (int) (seek_index + 7 + (2 * (no_of_records)));

			if (lastPosOfPointers < posToWrite) {
				insertIntoLeafPage(tableFile, table_name, seek_index, no_of_records, rowid, posToWrite,
						lastPosOfPointers, payload_size, columns, codes, entered_values, data_types);
			} else {
				long new_page_no = createPage(tableFile, true);

				tableFile.seek(seek_index + 4);
				tableFile.writeInt((int) new_page_no);

				int splitting_record = rowid;
				seek_index = new_page_no * pageSize;

				no_of_records = 1;

				tableFile.seek(seek_index + 2);
				lastWritePos = tableFile.readShort();

				record_size = 6 + payload_size;

				posToWrite = lastWritePos - record_size;

				lastPosOfPointers = (int) (seek_index + 7 + (2 * (no_of_records)));

				insertIntoLeafPage(tableFile, table_name, seek_index, no_of_records, rowid, posToWrite,
						lastPosOfPointers, payload_size, columns, codes, entered_values, data_types);

				boolean finished = false;

				while (!finished) {
					if (interiorPages.isEmpty()) {
						long newInteriorPageNo = createPage(tableFile, false);
						seek_index = newInteriorPageNo * pageSize;
						no_of_records = 1;

						tableFile.seek(seek_index + 2);
						lastWritePos = tableFile.readShort();

						record_size = 4 + 4;

						posToWrite = lastWritePos - record_size;

						lastPosOfPointers = (int) (seek_index + 7 + (2 * (no_of_records)));

						insertIntoInteriorPage(tableFile, table_name, seek_index, no_of_records, posToWrite,
								lastPosOfPointers, splitting_record - 1, original_page_no, new_page_no);

						RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile(
								"data/catalog/davisbase_tables.tbl", "rw");
						updateMetaTable(davisbaseTablesCatalog, table_name, (int) newInteriorPageNo, true, false);

						davisbaseTablesCatalog.close();
						finished = true;

					} else {
						seek_index = interiorPages.pop();

						tableFile.seek(seek_index + 1);
						no_of_records = tableFile.readByte() + 1;

						tableFile.seek(seek_index + 2);
						lastWritePos = tableFile.readShort();

						record_size = 4 + 4;

						posToWrite = lastWritePos - record_size;

						lastPosOfPointers = (int) (seek_index + 7 + (2 * (no_of_records)));

						if (lastPosOfPointers < posToWrite) {
							insertIntoInteriorPage(tableFile, table_name, seek_index, no_of_records, posToWrite,
									lastPosOfPointers, splitting_record - 1, original_page_no, new_page_no);
							finished = true;
						} else {

							original_page_no = (int) ((seek_index / pageSize));
							long newInteriorPageNo = createPage(tableFile, false);

							seek_index = newInteriorPageNo * pageSize;
							no_of_records = 1;

							tableFile.seek(seek_index + 2);
							lastWritePos = tableFile.readShort();

							record_size = 4 + 4;

							posToWrite = lastWritePos - record_size;

							lastPosOfPointers = (int) (seek_index + 7 + (2 * (no_of_records)));

							insertIntoInteriorPage(tableFile, table_name, seek_index, no_of_records, posToWrite,
									lastPosOfPointers, splitting_record - 1, original_page_no, new_page_no);

							new_page_no = newInteriorPageNo;
						}
					}
				}

			}

			tableFile.close();
		} catch (Exception e) {
			System.out.println(ERROR_MESSAGE);
		}

	}

	private static void insertIntoInteriorPage(RandomAccessFile tableFile, String table_name, long seek_index,
			int no_of_records, int posToWrite, int lastPosOfPointers, int splitting_record, int original_page_no,
			long new_page_no) {
		try {
			tableFile.seek(seek_index + 1);
			tableFile.writeByte(no_of_records);

			tableFile.seek(seek_index + 2);
			tableFile.writeShort(posToWrite);

			tableFile.seek(seek_index + 4);
			tableFile.writeInt((int) new_page_no);

			tableFile.seek(lastPosOfPointers - 1);
			tableFile.writeShort(posToWrite);

			tableFile.seek(posToWrite);
			tableFile.writeInt(splitting_record);
			tableFile.writeInt(original_page_no);

		} catch (Exception e) {
			System.out.println(ERROR_MESSAGE);
		}
	}

	private static void insertIntoLeafPage(RandomAccessFile tableFile, String table_name, long seek_index,
			int no_of_records, int rowid, int posToWrite, int lastPosOfPointers, int payload_size, int columns,
			ArrayList<Integer> codes, ArrayList<String> entered_values, ArrayList<String> data_types) {
		try {
			tableFile.seek(seek_index + 1);
			tableFile.writeByte(no_of_records);

			tableFile.seek(seek_index + 2);
			tableFile.writeShort(posToWrite);

			tableFile.seek(lastPosOfPointers - 1);
			tableFile.writeShort(posToWrite);

			tableFile.seek(posToWrite);
			tableFile.writeShort(payload_size);
			tableFile.writeInt(rowid);
			tableFile.writeByte(columns);

			for (int i = 0; i < columns; i++) {
				if (entered_values.get(i).equals("null")) {
					int[] codeAndSerial = calculateSerialCodeAndSize("null " + data_types.get(i));
					tableFile.writeByte(codeAndSerial[0]);
				} else {
					tableFile.writeByte(codes.get(i));
				}
			}

			for (int i = 0; i < columns; i++) {
				String method;

				if (entered_values.get(i).equals("null")) {
					method = calculateMethod("null " + data_types.get(i));
					entered_values.set(i, "0");
					if (method.equals("line")) {
						continue;
					}
				} else {
					method = calculateMethod(data_types.get(i));
				}

				if (method.equals("byte")) {
					tableFile.writeByte(Byte.parseByte(entered_values.get(i)));
				}
				if (method.equals("short")) {
					tableFile.writeShort(Short.parseShort(entered_values.get(i)));
				}
				if (method.equals("int")) {
					tableFile.writeInt(Integer.parseInt(entered_values.get(i)));
				}
				if (method.equals("long")) {
					if (data_types.get(i).equals("date") && !entered_values.get(i).equals("0")) {
						Date date = new SimpleDateFormat("yyyy-MM-dd").parse(entered_values.get(i));
						tableFile.writeLong((date.getTime()));
					} else if (data_types.get(i).equals("datetime") && !entered_values.get(i).equals("0")) {
						Date date = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss").parse(entered_values.get(i));
						tableFile.writeLong((date.getTime()));
					} else {
						tableFile.writeLong(Long.parseLong(entered_values.get(i)));
					}
				}
				if (method.equals("float")) {
					tableFile.writeFloat(Float.parseFloat(entered_values.get(i)));
				}
				if (method.equals("double")) {
					tableFile.writeDouble(Double.parseDouble(entered_values.get(i)));
				}
				if (method.equals("line")) {
					tableFile.writeBytes(entered_values.get(i));
				}
			}

			RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			updateMetaTable(davisbaseTablesCatalog, table_name, rowid, false, true);
			davisbaseTablesCatalog.close();
		} catch (Exception e) {
			System.out.println(ERROR_MESSAGE);
		}

	}

	public static void insertIntoMetaTable(String table_name, int record_count, short root_page, byte is_active) {

		try {
			RandomAccessFile davisbaseTablesCatalog = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			int rowid = getfromMetaTable("davisbase_tables", false, true) + 1;
			table_name = table_name + "\r";

			davisbaseTablesCatalog.seek(2);
			int lastWritePos = (davisbaseTablesCatalog.readShort());

			int name_length = table_name.length();
			int[] table_name_CS = calculateSerialCodeAndSize("text");
			table_name_CS[0] = table_name_CS[0] + name_length;
			table_name_CS[1] = name_length;

			int[] record_count_CS = calculateSerialCodeAndSize("int");
			int[] root_page_CS = calculateSerialCodeAndSize("smallint");
			int[] is_active_CS = calculateSerialCodeAndSize("tinyint");

			int payload_size = 1 + 4 + table_name_CS[1] + record_count_CS[1] + root_page_CS[1] + is_active_CS[1];
			int header_size = 6;
			int record_size = header_size + payload_size;

			int posToWrite = lastWritePos - record_size;

			int lastPosOfPointers = 7 + (2 * (rowid));

			if (lastPosOfPointers < posToWrite) {

				davisbaseTablesCatalog.seek(1);
				davisbaseTablesCatalog.writeByte(rowid);

				davisbaseTablesCatalog.seek(2);
				davisbaseTablesCatalog.writeShort(posToWrite);

				davisbaseTablesCatalog.seek(lastPosOfPointers - 1);
				davisbaseTablesCatalog.writeShort(posToWrite);

				davisbaseTablesCatalog.seek(posToWrite);
				davisbaseTablesCatalog.writeShort(payload_size);
				davisbaseTablesCatalog.writeInt(rowid);
				davisbaseTablesCatalog.writeByte(4);
				davisbaseTablesCatalog.writeByte(table_name_CS[0]);
				davisbaseTablesCatalog.writeByte(record_count_CS[0]);
				davisbaseTablesCatalog.writeByte(root_page_CS[0]);
				davisbaseTablesCatalog.writeByte(is_active_CS[0]);
				davisbaseTablesCatalog.writeBytes(table_name);
				davisbaseTablesCatalog.writeInt(record_count);
				davisbaseTablesCatalog.writeShort(root_page);
				davisbaseTablesCatalog.writeByte(is_active);

				updateMetaTable(davisbaseTablesCatalog, "davisbase_tables", rowid, false, true);

			}

			davisbaseTablesCatalog.close();

		} catch (Exception e) {
			System.out.println(ERROR_MESSAGE);
		}

	}

	/**
	 * @param s
	 *            The String to be repeated
	 * @param num
	 *            The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num
	 *         times.
	 */
	public static String line(String s, int num) {
		String a = "";
		for (int i = 0; i < num; i++) {
			a += s;
		}
		return a;
	}

	/**
	 * *********************************************************************** Main
	 * method
	 */
	public static void main(String[] args) {

		/* Display the welcome screen */
		splashScreen();

		/* This method will initialize the database storage if it doesn't exit */
		initializeDataStore();

		/* Variable to collect user input from the prompt */
		String userCommand = "";

		while (!isExit) {
			System.out.print(prompt);
			/* toLowerCase() renders command case insensitive */
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");

	}

	/**
	 * Stub method for creating new tables
	 *
	 * @param queryString
	 *            is a String of the user input
	 */
	public static void parseCreateTable(String createTableString) {

		ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));

		try {
			String table_name = createTableTokens.get(2);
			int root_page = getfromMetaTable(table_name, true, false);
			if (root_page > -1) {
				System.out.println("Table '" + table_name + "' already exists");
				return;
			}
			String tableFileName = createTableTokens.get(2) + ".tbl";

			ArrayList<String> attribute_names = new ArrayList<String>();
			ArrayList<String> data_types = new ArrayList<String>();
			ArrayList<String> constraints = new ArrayList<String>();
			String end = createTableTokens.get(3);
			int i = 0;
			int j = 4;
			while (!end.equals(")")) {
				attribute_names.add(createTableTokens.get(j++));
				data_types.add(createTableTokens.get(j++));
				if (createTableTokens.get(j).equals(",") || createTableTokens.get(j).equals(")")) {
					constraints.add("null");
				} else {
					constraints.add(createTableTokens.get(j++) + " " + createTableTokens.get(j++));
				}
				end = createTableTokens.get(j++);
				i++;

			}

			if (!attribute_names.get(0).equals("rowid")) {
				System.out.println("The first attribute name must be 'rowid' ");
				return;
			}
			if (!data_types.get(0).equals("int")) {
				System.out.println("The rowid attribute must be of 'INT' data type ");
				return;
			}
			if (!constraints.get(0).equals("primary key")) {
				System.out.println("rowid must only have 'PRIMARY KEY' constraint");
				return;
			}

			RandomAccessFile tableFile = new RandomAccessFile("data/user_data/" + tableFileName, "rw");
			tableFile.setLength(0);
			createPage(tableFile, true);
			tableFile.close();
			parseInsert(
					"insert into davisbase_tables values ( " + table_name + " , " + 0 + " , " + 0 + " , " + 1 + " )");
			for (int k = 0; k < i; k++) {
				root_page = getfromMetaTable("davisbase_columns", true, false);
				ArrayList<String> attributes = new ArrayList<String>();
				attributes.add(table_name);
				attributes.add(attribute_names.get(k));
				attributes.add(data_types.get(k));
				attributes.add(String.valueOf(k + 1));
				attributes.add(constraints.get(k).equals("null") ? "YES" : "NO");
				attributes.add("1");

				ArrayList<String> types = new ArrayList<String>();
				types.add("text");
				types.add("text");
				types.add("text");
				types.add("tinyint");
				types.add("text");
				types.add("tinyint");

				insertIntoBtree("davisbase_columns", "data/catalog/davisbase_columns.tbl", root_page, attributes, types,
						attributes);
			}
		} catch (Exception e) {
			System.out.println(ERROR_MESSAGE);
			return;
		}

	}

	public static void parseInsert(String insertString) {
		ArrayList<String> insertTokens = new ArrayList<String>(Arrays.asList(insertString.split(" ")));

		try {
			String table_name = insertTokens.get(2);
			int root_page = -1;
			String table_file_name = null;
			if (table_name.equals("davisbase_tables")) {
				table_file_name = "data/catalog/" + table_name + ".tbl";
			} else {

				table_file_name = "data/user_data/" + table_name + ".tbl";
			}

			root_page = getfromMetaTable(table_name, true, false);

			if (root_page == -1) {
				System.out.println("Table '" + table_name + "' doesnt exist");
				return;
			}

			ArrayList<String> attribute_names = new ArrayList<String>();
			ArrayList<String> data_types = new ArrayList<String>();
			ArrayList<String> constraints = new ArrayList<String>();

			getColumnMetaData(table_name, attribute_names, data_types, constraints);

			ArrayList<String> entered_attribute_names = new ArrayList<String>();
			ArrayList<String> entered_values = new ArrayList<String>();
			ArrayList<String> temp_entered_values = new ArrayList<String>();

			if (insertTokens.get(3).equals("(")) {
				int j = 4;
				String end = insertTokens.get(j);
				while (!end.equals(")")) {
					entered_attribute_names.add(insertTokens.get(j++));
					end = insertTokens.get(j++);
				}
				j++; // values
				j++; // (
				end = insertTokens.get(j);
				while (!end.equals(")")) {
					String[] tempArray = null;
					String tempValue = insertTokens.get(j++);
					if (tempValue.startsWith("'")) {
						tempArray = tempValue.split("'");
					}
					if (tempValue.startsWith("\"")) {
						tempArray = tempValue.split("\"");
					}

					if ((tempArray != null) && (tempArray.length > 1)) {
						tempValue = tempArray[1];
					} else if ((tempArray != null) && (tempArray.length == 1)) {
						tempValue = tempArray[0];
					}
					temp_entered_values.add(tempValue);
					end = insertTokens.get(j++);
				}
				if (entered_attribute_names.size() != temp_entered_values.size()) {
					System.out.println("Field and value list must have same number of entries");
					return;
				} else {
					for (int r = 0; r < attribute_names.size(); r++) {
						if (constraints.get(r).equalsIgnoreCase("No")) {
							if (!entered_attribute_names.contains(attribute_names.get(r))) {
								System.out.println("Not null attributes must be specified in the insert");
								return;
							}
						}
					}
					for (int r = 0; r < entered_attribute_names.size(); r++) {
						for (int s = 0; s < attribute_names.size(); s++) {
							if (attribute_names.get(s).equals(entered_attribute_names.get(r))) {
								if (constraints.get(s).equalsIgnoreCase("No")) {
									if (temp_entered_values.get(r).equals("null")) {
										System.out.println("Column '" + attribute_names.get(s) + "' cannot be null");
										return;
									}
								}
							}
						}
					}

					for (int r = 0; r < attribute_names.size(); r++) {
						boolean added = false;
						for (int s = 0; s < entered_attribute_names.size(); s++) {
							if (attribute_names.get(r).equals(entered_attribute_names.get(s))) {
								entered_values.add(r, temp_entered_values.get(s));
								added = true;
							}
						}
						if (!added) {
							entered_values.add(r, "null");
						}
					}
					for (int r = 0; r < entered_attribute_names.size(); r++) {
						if (!attribute_names.contains(entered_attribute_names.get(r))) {
							System.out
									.println("Unknown column '" + entered_attribute_names.get(r) + "' in 'field list'");
							return;
						}
					}

				}
			} else {
				int j = 5;
				String end = insertTokens.get(j);
				while (!end.equals(")")) {
					String[] tempArray = null;
					String tempValue = insertTokens.get(j++);
					if (tempValue.startsWith("'")) {
						tempArray = tempValue.split("'");
					}
					if (tempValue.startsWith("\"")) {
						tempArray = tempValue.split("\"");
					}

					if ((tempArray != null) && (tempArray.length > 1)) {
						tempValue = tempArray[1];
					} else if ((tempArray != null) && (tempArray.length == 1)) {
						tempValue = tempArray[0];
					}
					entered_values.add(tempValue);
					end = insertTokens.get(j++);
				}

				if (entered_values.size() != attribute_names.size()) {
					if (entered_values.size() < attribute_names.size()) {
						for (int r = 0; r < entered_values.size(); r++) {
							if (constraints.get(r).equalsIgnoreCase("No")) {
								if (entered_values.get(r).equals("null")) {
									System.out.println("Column '" + attribute_names.get(r) + "' cannot be null");
									return;
								}
							}
						}
						int size = entered_values.size();
						for (int r = size; r < attribute_names.size(); r++) {
							if (constraints.get(r).equalsIgnoreCase("no")) {
								System.out.println("Not null columns must be specified in the 'value list'");
								return;
							} else {
								entered_values.add(r, "null");
							}
						}
					} else {
						System.out.println(ERROR_MESSAGE);
						return;
					}

				} else {
					for (int r = 0; r < attribute_names.size(); r++) {
						if (constraints.get(r).equalsIgnoreCase("No")) {
							if (entered_values.get(r).equals("null")) {
								System.out.println("Column '" + attribute_names.get(r) + "' cannot be null");
								return;
							}
						}
					}
				}

			}

			if (entered_values.size() != attribute_names.size()) {
				System.out.println(ERROR_MESSAGE);
				return;
			}

			insertIntoBtree(table_name, table_file_name, root_page, attribute_names, data_types, entered_values);

			System.out.println("Operation successful");

		} catch (Exception e) {
			System.out.println(ERROR_MESSAGE);
		}
	}

	/**
	 * Stub method for executing queries
	 *
	 * @param queryString
	 *            is a String of the user input
	 */
	public static void parseQuery(String queryString) {

		ArrayList<String> selectTokens = new ArrayList<String>(Arrays.asList(queryString.split(" ")));
		ArrayList<String> projectiles = new ArrayList<String>();
		boolean projectAll = false;
		boolean whereClauseIncluded = false;
		String whereAttribute = null;
		String whereOperator = null;
		String whereValue = null;
		String table_name = null;
		try {
			if (selectTokens.get(1).equals("*")) {
				table_name = selectTokens.get(3);
				projectAll = true;
				if (!selectTokens.get(2).equals("from")) {
					System.out.println(ERROR_MESSAGE);
					return;
				}

			} else {
				projectAll = false;
				for (int i = 1; i < selectTokens.size(); i++) {
					if (selectTokens.get(i).equals("from")) {
						table_name = selectTokens.get(i + 1);
						break;
					} else if (!selectTokens.get(i).equals(",")) {
						projectiles.add(selectTokens.get(i));
					}
				}
			}

			for (int i = 3; i < selectTokens.size(); i++) {
				if (selectTokens.get(i).equals("where")) {
					whereClauseIncluded = true;
					whereAttribute = selectTokens.get(i + 1);
					whereOperator = selectTokens.get(i + 2);
					if (whereOperator.equals("is") && selectTokens.get(i + 3).equals("not")
							&& (selectTokens.size() == (i + 5))) {
						whereOperator += " " + selectTokens.get(i + 3);
						whereValue = selectTokens.get(i + 4);
					} else {
						whereValue = selectTokens.get(i + 3);
					}
					break;
				}
			}

			if (whereValue != null) {
				String[] tempWhereValue = null;

				if (whereValue.startsWith("'")) {
					tempWhereValue = whereValue.split("'");
				}
				if (whereValue.startsWith("\"")) {
					tempWhereValue = whereValue.split("\"");
				}

				if ((tempWhereValue != null) && (tempWhereValue.length > 1)) {
					whereValue = tempWhereValue[1];
				} else if ((tempWhereValue != null) && (tempWhereValue.length == 1)) {
					whereValue = tempWhereValue[0];
				}
			}

			String table_file_name = null;
			if (table_name.equals("davisbase_tables") || table_name.equals("davisbase_columns")) {
				table_file_name = "data/catalog/" + table_name + ".tbl";
			} else {
				table_file_name = "data/user_data/" + table_name + ".tbl";
			}
			read_table(table_name, table_file_name, projectAll, whereClauseIncluded, projectiles, whereAttribute,
					whereOperator, whereValue, (whereAttribute != null) && whereAttribute.equals("rowid"));

		} catch (Exception e) {
			System.out.println(ERROR_MESSAGE);
		}

	}

	/**
	 * Stub method for updating records
	 *
	 * @param updateString
	 *            is a String of the user input
	 */
	public static void parseUpdate(String updateString) {
		ArrayList<String> updateTableTokens = new ArrayList<String>(Arrays.asList(updateString.split(" ")));
		boolean whereClauseIncluded = false;
		String whereAttribute = null;
		String whereOperator = null;
		String whereValue = null;
		String table_name = null;
		String updateAttribute = null;
		String updateOperator = null;
		String updateValue = null;

		try {

			table_name = updateTableTokens.get(1);
			updateAttribute = updateTableTokens.get(3);
			updateOperator = updateTableTokens.get(4);
			updateValue = updateTableTokens.get(5);

			if ((updateTableTokens.size() > 6) && updateTableTokens.get(6).equals("where")) {
				whereClauseIncluded = true;
				whereAttribute = updateTableTokens.get(7);
				whereOperator = updateTableTokens.get(8);
				if (whereOperator.equals("is") && updateTableTokens.get(9).equals("not")
						&& (updateTableTokens.size() == 11)) {
					whereOperator += " " + updateTableTokens.get(9);
					whereValue = updateTableTokens.get(10);
				} else {
					whereValue = updateTableTokens.get(9);
				}
			}

			if (updateAttribute.equalsIgnoreCase("rowid")) {
				System.out.println("Cannot update the primary key used as an index");
				return;
			}

			String[] tempUpdateValue = null;

			if (updateValue.startsWith("'")) {
				tempUpdateValue = updateValue.split("'");
			}
			if (updateValue.startsWith("\"")) {
				tempUpdateValue = updateValue.split("\"");
			}

			if ((tempUpdateValue != null) && (tempUpdateValue.length > 1)) {
				updateValue = tempUpdateValue[1];
			} else if ((tempUpdateValue != null) && (tempUpdateValue.length == 1)) {
				updateValue = tempUpdateValue[0];
			}

			if (whereValue != null) {
				String[] tempWhereValue = null;

				if (whereValue.startsWith("'")) {
					tempWhereValue = whereValue.split("'");
				}
				if (whereValue.startsWith("\"")) {
					tempWhereValue = whereValue.split("\"");
				}

				if ((tempWhereValue != null) && (tempWhereValue.length > 1)) {
					whereValue = tempWhereValue[1];
				} else if ((tempWhereValue != null) && (tempWhereValue.length == 1)) {
					whereValue = tempWhereValue[0];
				}
			}

			if (!updateOperator.equals("=")) {
				System.out.println(ERROR_MESSAGE);
				return;
			}

			String table_file_name = null;
			if (table_name.equals("davisbase_tables") || table_name.equals("davisbase_columns")) {
				System.out.println("Cannot update system tables");
				return;
			} else {
				table_file_name = "data/user_data/" + table_name + ".tbl";
			}
			update_table(table_name, table_file_name, updateAttribute, updateOperator, updateValue, whereClauseIncluded,
					whereAttribute, whereOperator, whereValue,
					(whereAttribute != null) && whereAttribute.equals("rowid"));

		} catch (Exception e) {
			System.out.println(ERROR_MESSAGE);
		}
	}

	public static void parseUserCommand(String userCommand) {

		/*
		 * commandTokens is an array of Strings that contains one token per array
		 * element The first token can be used to determine the type of command The
		 * other tokens can be used to pass relevant parameters to each command-specific
		 * method inside each case statement
		 */
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

		switch (commandTokens.get(0)) {
		case "select":
			parseQuery(userCommand);
			break;
		case "drop":
			dropTable(userCommand);
			break;
		case "create":
			parseCreateTable(userCommand);
			break;
		case "update":
			parseUpdate(userCommand);
			break;
		case "insert":
			parseInsert(userCommand);
			break;
		case "help":
			help();
			break;
		case "show":
			if ((commandTokens.size() == 2) && commandTokens.get(1).equals("tables")) {
				parseQuery("select table_name from davisbase_tables where is_active != 0");
			} else {
				System.out.println(ERROR_MESSAGE);
			}
			break;
		case "version":
			displayVersion();
			break;
		case "exit":
			isExit = true;
			break;
		case "quit":
			isExit = true;
		default:
			System.out.println(ERROR_MESSAGE);
			break;
		}
	}

	public static void printCmd(String s) {
		System.out.println("\n\t" + s + "\n");
	}

	public static void printDef(String s) {
		System.out.println("\t\t" + s);
	}

	private static void read_table(String table_name, String table_file_name, boolean projectAll,
			boolean whereClauseIncluded, ArrayList<String> projectiles, String whereAttribute, String whereOperator,
			String whereValue, boolean useIndexing) {
		try {

			int root_page = getfromMetaTable(table_name, true, false);
			if (root_page == -1) {
				System.out.println("Table '" + table_name + "' doesnt exist");
				return;
			}

			ArrayList<String> attribute_names = new ArrayList<String>();
			attribute_names.add("rowid");
			ArrayList<String> data_types = new ArrayList<String>();
			data_types.add("int");
			ArrayList<String> constraints = new ArrayList<String>();
			constraints.add("NO");

			getColumnMetaData(table_name, attribute_names, data_types, constraints);

			if (!attribute_names.contains(whereAttribute) && (whereAttribute != null)) {
				System.out.println("Unknown column '" + whereAttribute + "' in 'where clause'");
				return;
			}

			RandomAccessFile tableFile = new RandomAccessFile(table_file_name, "r");

			String line = null;
			boolean printingFirstTime = true;
			boolean whereValueChanged = false;

			int seek = 0;
			int right_child_pointer = 0;
			int page = 0;
			int records = 0;
			int endPage = Integer.MAX_VALUE - 1;
			int row_count = 0;

			if (useIndexing) {
				page = root_page;
				seek = (int) (page * pageSize);
				tableFile.seek(seek);
				while (tableFile.readByte() != 13) {
					tableFile.seek(seek + 1);
					records = tableFile.readByte();

					int posToRead = seek + 6 + (2 * records);
					tableFile.seek(posToRead);

					posToRead = tableFile.readShort();
					tableFile.seek(posToRead);
					int firstRowId = tableFile.readInt();
					int secondRowId = firstRowId;
					if (Integer.valueOf(whereValue) <= firstRowId) {
						int originalPage = tableFile.readInt();
						while ((records > 1) && (Integer.valueOf(whereValue) < secondRowId)) {
							records--;
							posToRead = seek + 6 + (2 * records);
							tableFile.seek(posToRead);
							posToRead = tableFile.readShort();
							tableFile.seek(posToRead);
							firstRowId = tableFile.readInt();
							originalPage = tableFile.readInt();

							if (records != 1) {
								records--;
								posToRead = seek + 6 + (2 * records);
								tableFile.seek(posToRead);
								posToRead = tableFile.readShort();
								tableFile.seek(posToRead);
								secondRowId = tableFile.readInt();
								records++;
							}
						}
						page = originalPage;
					} else {
						tableFile.seek(seek + 4);
						page = tableFile.readInt();
					}
					seek = (int) (page * pageSize);
					tableFile.seek(seek);
				}
				if (whereOperator.contains("<")) {
					endPage = page;
					page = 0;
				}
				if (whereOperator.equals("=")) {
					endPage = page;
				}
			}

			while ((right_child_pointer != -1) && (right_child_pointer < (endPage + 1))) {
				seek = (int) (page * pageSize);
				tableFile.seek(seek + 4);
				right_child_pointer = tableFile.readInt();
				page = right_child_pointer;
				tableFile.seek(seek + 1);
				records = tableFile.readByte();

				for (int i = 0; i < records; i++) {
					ArrayList<String> values = new ArrayList<String>();
					int posToRead = seek + 8 + (2 * i);
					tableFile.seek(posToRead);
					posToRead = tableFile.readShort();

					tableFile.seek(posToRead + 6);
					int columns = tableFile.readByte();

					String[] testValues = new String[columns + 1];
					tableFile.seek(posToRead + 2);

					testValues[0] = String.valueOf(tableFile.readInt());

					int bytesRead = 0;

					for (int k = 0; k < columns; k++) {
						tableFile.seek(posToRead + 6 + 1 + k);

						int code = tableFile.readByte();
						String type = calculateDatatype(code);
						int[] codeAndSize = calculateSerialCodeAndSize(type);

						tableFile.seek(posToRead + 6 + 1 + columns + bytesRead);
						String method;

						if (codeAndSize[0] < 4) {
							method = "null";
						} else {
							method = calculateMethod(type);
						}

						if (method.equals("null")) {
							testValues[k + 1] = "null";
						}
						if (method.equals("byte")) {
							testValues[k + 1] = String.valueOf(tableFile.readByte());
						}
						if (method.equals("short")) {
							testValues[k + 1] = String.valueOf(tableFile.readShort());
						}
						if (method.equals("int")) {
							testValues[k + 1] = String.valueOf(tableFile.readInt());
						}
						if (method.equals("long")) {
							testValues[k + 1] = String.valueOf(tableFile.readLong());
						}
						if (method.equals("float")) {
							testValues[k + 1] = String.valueOf(tableFile.readFloat());
						}
						if (method.equals("double")) {
							testValues[k + 1] = String.valueOf(tableFile.readDouble());
						}
						if (method.equals("line")) {
							String temp = String.valueOf(tableFile.readLine());
							if (temp.equals("")) {
								testValues[k + 1] = "null";
							} else {
								testValues[k + 1] = temp;
							}

						}

						if (type.equals("text")) {
							bytesRead = (bytesRead + code) - codeAndSize[0];
						} else {
							bytesRead = bytesRead + codeAndSize[1];
						}
					}
					boolean include = true;

					if (whereClauseIncluded) {
						include = false;
						for (int l = 0; l < attribute_names.size(); l++) {
							if (attribute_names.get(l).equalsIgnoreCase(whereAttribute)) {
								if (data_types.get(l).equals("date") && !whereValueChanged
										&& !whereValue.equals("null")) {
									Date date = new SimpleDateFormat("yyyy-MM-dd").parse(whereValue);
									whereValue = String.valueOf(date.getTime());
									whereValueChanged = true;
								} else if (data_types.get(l).equals("datetime") && !whereValueChanged
										&& !whereValue.equals("null")) {
									Date date = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss").parse(whereValue);
									whereValue = String.valueOf(date.getTime());
									whereValueChanged = true;
								}
								switch (whereOperator) {
								case "=": {
									if (data_types.get(l).equalsIgnoreCase(("text"))) {
										if (whereValue.equals("null")) {
											include = false;
										} else {
											include = testValues[l].equalsIgnoreCase(whereValue);
										}
									} else {
										if (whereValue.equals("null")) {
											include = false;
										} else if (testValues[l].equals("null")) {
											include = false;
										} else {
											include = Double.valueOf(testValues[l]).equals(Double.valueOf(whereValue));
										}
									}
									break;
								}
								case "is": {
									if (whereValue.equals("null")) {
										include = testValues[l].equals("null");
									}
									break;
								}

								case "<": {
									if (!testValues[l].equals("null")) {
										if (data_types.get(l).equals("text")) {
											include = testValues[l].compareTo(whereValue) < 0;
										} else {
											if (!whereValue.equals("null")) {
												include = Double.valueOf(testValues[l])
														.compareTo(Double.valueOf(whereValue)) < 0;
											}
										}
									}
									break;
								}
								case "<=": {
									if (!testValues[l].equals("null")) {
										if (data_types.get(l).equals("text")) {
											include = testValues[l].compareTo(whereValue) <= 0;
										} else {
											if (!whereValue.equals("null")) {
												include = Double.valueOf(testValues[l])
														.compareTo(Double.valueOf(whereValue)) <= 0;
											}
										}
									}
									break;
								}
								case ">": {
									if (!testValues[l].equals("null")) {
										if (data_types.get(l).equals("text")) {
											include = testValues[l].compareTo(whereValue) > 0;
										} else {
											if (!whereValue.equals("null")) {
												include = Double.valueOf(testValues[l])
														.compareTo(Double.valueOf(whereValue)) > 0;
											}
										}
									}
									break;
								}
								case ">=": {
									if (!testValues[l].equals("null")) {
										if (data_types.get(l).equals("text")) {
											include = testValues[l].compareTo(whereValue) >= 0;
										} else {
											if (!whereValue.equals("null")) {
												include = Double.valueOf(testValues[l])
														.compareTo(Double.valueOf(whereValue)) >= 0;
											}
										}
									}
									break;
								}
								case "<>":
								case "!=": {
									if (data_types.get(l).equals("text")) {
										if (whereValue.equals("null")) {
											include = false;
										} else {
											include = !testValues[l].equalsIgnoreCase(whereValue);
										}
									} else {
										if (whereValue.equals("null")) {
											include = false;
										} else if (testValues[l].equals("null")) {
											include = false;
										} else {
											include = Double.valueOf(testValues[l])
													.compareTo(Double.valueOf(whereValue)) != 0;
										}
									}
									break;
								}
								case "is not": {
									if (whereValue.equals("null")) {
										include = !testValues[l].equals("null");
									}
									break;
								}
								case "like": {
									if (data_types.get(l).equals("text") && !testValues[l].equals("null")) {
										String tempValue = whereValue;
										if (tempValue.contains("%")) {
											tempValue = tempValue.toLowerCase();
											tempValue = tempValue.replace("_", ".");
											tempValue = tempValue.replace("%", ".*");
											testValues[l] = testValues[l].toLowerCase();
											include = testValues[l].matches(tempValue);
										} else {
											include = testValues[l].equals(whereValue);
										}
									}
									break;
								}
								default: {
									System.out.println(ERROR_MESSAGE);
									return;
								}
								}
							}
						}
					}

					if (projectAll && printingFirstTime) {
						System.out.println();
						line = line("%-" + ((attribute_names.size() * 20) / attribute_names.size()) + "s",
								attribute_names.size()) + "\n";
						System.out.format(line, attribute_names.toArray());
						System.out.println(line("-", (attribute_names.size() * 20)));
						printingFirstTime = false;
					} else if (printingFirstTime) {
						for (int s = 0; s < projectiles.size(); s++) {
							if (!attribute_names.contains(projectiles.get(s))) {
								System.out.println("Unknown column '" + projectiles.get(s) + "' in 'field list' ");
								tableFile.close();
								return;
							}
						}
						System.out.println();
						line = line("%-" + ((projectiles.size() * 20) / projectiles.size()) + "s", projectiles.size())
								+ "\n";
						System.out.format(line, projectiles.toArray());
						System.out.println(line("-", (projectiles.size() * 20)));
						printingFirstTime = false;
					}

					if (include) {
						for (int l = 0; l < attribute_names.size(); l++) {
							if (data_types.get(l).equals("date") && (testValues[l] != "null")) {
								Date date = new Date(Long.parseLong(testValues[l]));
								SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
								testValues[l] = df.format(date);
							} else if (data_types.get(l).equals("datetime") && (testValues[l] != "null")) {
								Date date = new Date(Long.parseLong(testValues[l]));
								SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss");
								testValues[l] = df.format(date);
							}
						}
						if (projectAll) {
							System.out.format(line, (Object[]) testValues);
							row_count++;
						} else {
							for (int k = 0; k < projectiles.size(); k++) {
								for (int l = 0; l < attribute_names.size(); l++) {
									if (attribute_names.get(l).equals(projectiles.get(k))) {
										values.add(testValues[l]);
									}
								}
							}
							System.out.format(line, values.toArray());
							row_count++;
						}
					}
				}
			}
			System.out.println();
			System.out.println(row_count + " row(s) returned");
			System.out.println();

			tableFile.close();
		} catch (Exception e) {
			if (e instanceof FileNotFoundException) {
				System.out.println("Table '" + table_name + "' doesnt exist");
			} else {
				System.out.println(ERROR_MESSAGE);
			}
		}
	}

	/**
	 * Display the splash screen
	 */
	public static void splashScreen() {
		System.out.println(line("-", 80));
		System.out.println("Welcome to DavisBaseLite"); // Display the string.
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-", 80));
	}

	private static void update_table(String table_name, String table_file_name, String updateAttribute,
			String updateOperator, String updateValue, boolean whereClauseIncluded, String whereAttribute,
			String whereOperator, String whereValue, boolean useIndexing) {

		int root_page = getfromMetaTable(table_name, true, false);
		if (root_page == -1) {
			System.out.println("Table '" + table_name + "' doesnt exist");
			return;
		}

		try {
			ArrayList<String> attribute_names = new ArrayList<String>();
			attribute_names.add("rowid");
			ArrayList<String> data_types = new ArrayList<String>();
			data_types.add("int");
			ArrayList<String> constraints = new ArrayList<String>();
			constraints.add("NO");

			getColumnMetaData(table_name, attribute_names, data_types, constraints);

			if (!attribute_names.contains(updateAttribute)) {
				System.out.println("Unknown column '" + updateAttribute + "' in 'field list'");
			}
			if (!attribute_names.contains(whereAttribute) && (whereAttribute != null)) {
				System.out.println("Unknown column '" + whereAttribute + "' in 'where clause'");
				return;
			}

			for (int s = 0; s < attribute_names.size(); s++) {
				if (attribute_names.get(s).equals(updateAttribute)) {
					if (constraints.get(s).equalsIgnoreCase("No")) {
						if (updateValue.equals("null")) {
							System.out.println("Column '" + attribute_names.get(s) + "' cannot be null");
							return;
						}
					}
				}
			}

			RandomAccessFile tableFile = new RandomAccessFile(table_file_name, "rw");

			int seek = 0;
			int right_child_pointer = 0;
			int page = 0;
			int records = 0;
			int endPage = Integer.MAX_VALUE - 1;
			boolean updateValueChanged = false;
			boolean whereValueChanged = false;
			int changed_rows = 0;

			if (useIndexing) {
				page = root_page;
				seek = (int) (page * pageSize);
				tableFile.seek(seek);
				while (tableFile.readByte() != 13) {
					tableFile.seek(seek + 1);
					records = tableFile.readByte();

					int posToRead = seek + 6 + (2 * records);
					tableFile.seek(posToRead);

					posToRead = tableFile.readShort();
					tableFile.seek(posToRead);
					int firstRowId = tableFile.readInt();
					int secondRowId = firstRowId;
					if (Integer.valueOf(whereValue) <= firstRowId) {
						int originalPage = tableFile.readInt();
						while ((records > 1) && (Integer.valueOf(whereValue) < secondRowId)) {
							records--;
							posToRead = seek + 6 + (2 * records);
							tableFile.seek(posToRead);
							posToRead = tableFile.readShort();
							tableFile.seek(posToRead);
							firstRowId = tableFile.readInt();
							originalPage = tableFile.readInt();

							if (records != 1) {
								records--;
								posToRead = seek + 6 + (2 * records);
								tableFile.seek(posToRead);
								posToRead = tableFile.readShort();
								tableFile.seek(posToRead);
								secondRowId = tableFile.readInt();
								records++;
							}
						}
						page = originalPage;
					} else {
						tableFile.seek(seek + 4);
						page = tableFile.readInt();
					}
					seek = (int) (page * pageSize);
					tableFile.seek(seek);
				}
				if (whereOperator.contains("<")) {
					endPage = page;
					page = 0;
				}
				if (whereOperator.equals("=")) {
					endPage = page;
				}
			}
			int columnIndexToUpdate = 0;
			int posToUpdateValue = 0;
			int posToUpdateCode = 0;
			String updateType = null;

			for (int l = 1; l < attribute_names.size(); l++) {
				if (attribute_names.get(l).equalsIgnoreCase(updateAttribute)) {
					columnIndexToUpdate = l - 1;
				}
			}

			while ((right_child_pointer != -1) && (right_child_pointer < (endPage + 1))) {
				seek = (int) (page * pageSize);
				tableFile.seek(seek + 4);
				right_child_pointer = tableFile.readInt();
				page = right_child_pointer;
				tableFile.seek(seek + 1);
				records = tableFile.readByte();
				for (int i = 0; i < records; i++) {
					int posToRead = seek + 8 + (2 * i);
					tableFile.seek(posToRead);
					posToRead = tableFile.readShort();

					tableFile.seek(posToRead + 6);
					int columns = tableFile.readByte();

					String[] testValues = new String[columns + 1];
					tableFile.seek(posToRead + 2);

					testValues[0] = String.valueOf(tableFile.readInt());

					int bytesRead = 0;

					for (int k = 0; k < columns; k++) {
						tableFile.seek(posToRead + 6 + 1 + k);

						int code = tableFile.readByte();
						String type = calculateDatatype(code);
						int[] codeAndSize = calculateSerialCodeAndSize(type);

						if (k == columnIndexToUpdate) {
							posToUpdateCode = posToRead + 6 + 1 + k;
							posToUpdateValue = posToRead + 6 + 1 + columns + bytesRead;
							updateType = type;
						}
						tableFile.seek(posToRead + 6 + 1 + columns + bytesRead);
						String method;

						if (codeAndSize[0] < 4) {
							method = "null";
						} else {
							method = calculateMethod(type);
						}

						if (method.equals("null")) {
							testValues[k + 1] = "null";
						}
						if (method.equals("byte")) {
							testValues[k + 1] = String.valueOf(tableFile.readByte());
						}
						if (method.equals("short")) {
							testValues[k + 1] = String.valueOf(tableFile.readShort());
						}
						if (method.equals("int")) {
							testValues[k + 1] = String.valueOf(tableFile.readInt());
						}
						if (method.equals("long")) {
							testValues[k + 1] = String.valueOf(tableFile.readLong());
						}
						if (method.equals("float")) {
							testValues[k + 1] = String.valueOf(tableFile.readFloat());
						}
						if (method.equals("double")) {
							testValues[k + 1] = String.valueOf(tableFile.readDouble());
						}
						if (method.equals("line")) {
							testValues[k + 1] = String.valueOf(tableFile.readLine());
							if (testValues[k + 1].equals("")) {
								testValues[k + 1] = "null";
							}
						}

						if (type.equals("text")) {
							bytesRead = (bytesRead + code) - codeAndSize[0];
						} else {
							bytesRead = bytesRead + codeAndSize[1];
						}
					}
					boolean update = true;

					if (whereClauseIncluded) {
						update = false;
						for (int l = 0; l < attribute_names.size(); l++) {
							if (attribute_names.get(l).equalsIgnoreCase(whereAttribute)) {
								if (data_types.get(l).equals("date") && !whereValueChanged
										&& !whereValue.equals("null")) {
									Date date = new SimpleDateFormat("yyyy-MM-dd").parse(whereValue);
									whereValue = String.valueOf(date.getTime());
									whereValueChanged = true;
								} else if (data_types.get(l).equals("datetime") && !whereValueChanged
										&& !whereValue.equals("null")) {
									Date date = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss").parse(whereValue);
									whereValue = String.valueOf(date.getTime());
									whereValueChanged = true;
								}
								switch (whereOperator) {
								case "=": {
									if (data_types.get(l).equalsIgnoreCase(("text"))) {
										if (whereValue.equals("null")) {
											update = false;
										} else {
											update = testValues[l].equalsIgnoreCase(whereValue);
										}
									} else {
										if (whereValue.equals("null")) {
											update = false;
										} else if (testValues[l].equals("null")) {
											update = false;
										} else {
											update = Double.valueOf(testValues[l]).equals(Double.valueOf(whereValue));
										}
									}
									break;
								}
								case "is": {
									if (whereValue.equals("null")) {
										update = testValues[l].equals("null");
									}
									break;
								}
								case "<": {
									if (!testValues[l].equals("null")) {
										if (data_types.get(l).equals("text")) {
											update = testValues[l].compareTo(whereValue) < 0;
										} else {
											if (!whereValue.equals("null")) {
												update = Double.valueOf(testValues[l])
														.compareTo(Double.valueOf(whereValue)) < 0;
											}
										}
									}
									break;
								}
								case "<=": {
									if (!testValues[l].equals("null")) {
										if (data_types.get(l).equals("text")) {
											update = testValues[l].compareTo(whereValue) <= 0;
										} else {
											if (!whereValue.equals("null")) {
												update = Double.valueOf(testValues[l])
														.compareTo(Double.valueOf(whereValue)) <= 0;
											}
										}
									}
									break;
								}
								case ">": {
									if (!testValues[l].equals("null")) {
										if (data_types.get(l).equals("text")) {
											update = testValues[l].compareTo(whereValue) > 0;
										} else {
											if (!whereValue.equals("null")) {
												update = Double.valueOf(testValues[l])
														.compareTo(Double.valueOf(whereValue)) > 0;
											}
										}
									}
									break;
								}
								case ">=": {
									if (!testValues[l].equals("null")) {
										if (data_types.get(l).equals("text")) {
											update = testValues[l].compareTo(whereValue) >= 0;
										} else {
											if (!whereValue.equals("null")) {
												update = Double.valueOf(testValues[l])
														.compareTo(Double.valueOf(whereValue)) >= 0;
											}
										}
									}
									break;
								}
								case "<>":
								case "!=": {
									if (data_types.get(l).equals("text")) {
										if (whereValue.equals("null")) {
											update = false;
										} else {
											update = !testValues[l].equalsIgnoreCase(whereValue);
										}
									} else {
										if (whereValue.equals("null")) {
											update = false;
										} else if (testValues[l].equals("null")) {
											update = false;
										} else {
											update = Double.valueOf(testValues[l])
													.compareTo(Double.valueOf(whereValue)) != 0;
										}
									}
									break;
								}
								case "is not": {
									if (whereValue.equals("null")) {
										update = !testValues[l].equals("null");
									}
									break;
								}
								case "like": {
									if (data_types.get(l).equals("text") && !testValues[l].equals("null")) {
										String tempValue = whereValue;
										if (tempValue.contains("%")) {
											tempValue = tempValue.toLowerCase();
											tempValue = tempValue.replace("_", ".");
											tempValue = tempValue.replace("%", ".*");
											testValues[l] = testValues[l].toLowerCase();
											update = testValues[l].matches(tempValue);
										} else {
											update = testValues[l].equals(whereValue);
										}
									}
									break;
								}
								default: {
									System.out.println(ERROR_MESSAGE);
									return;
								}
								}
							}
						}
					}

					if (update) {
						for (int l = 1; l < attribute_names.size(); l++) {
							if (attribute_names.get(l).equalsIgnoreCase(updateAttribute)) {
								if (data_types.get(l).equals("text") && !updateValueChanged) {
									updateValue += "\r";
									updateValueChanged = true;
								}
								if (data_types.get(l).equals("date") && !updateValueChanged
										&& !updateValue.equals("null")) {
									Date date = new SimpleDateFormat("yyyy-MM-dd").parse(updateValue);
									updateValue = String.valueOf(date.getTime());
									updateValueChanged = true;
								} else if (data_types.get(l).equals("datetime") && !updateValueChanged
										&& !updateValue.equals("null")) {
									Date date = new SimpleDateFormat("yyyy-MM-dd_hh:mm:ss").parse(updateValue);
									updateValue = String.valueOf(date.getTime());
									updateValueChanged = true;
								}
								String method = calculateMethod(updateType);
								if (updateType.startsWith("null")) {
									if (!updateValue.equals("null")) {
										method = calculateMethod(data_types.get(l));
									}
								}
								if (method.equals("byte")) {
									if (!updateValue.equals("null")) {
										tableFile.seek(posToUpdateValue);
										tableFile.writeByte(Byte.parseByte(updateValue));
										tableFile.seek(posToUpdateCode);
										tableFile.writeByte(calculateSerialCodeAndSize(data_types.get(l))[0]);
										changed_rows++;
									} else if (updateValue.equals("null")) {
										tableFile.seek(posToUpdateCode);
										tableFile.writeByte(0);
										changed_rows++;
									}
								} else if (method.equals("short")) {
									if (!updateValue.equals("null")) {
										tableFile.seek(posToUpdateValue);
										tableFile.writeShort(Short.parseShort(updateValue));
										tableFile.seek(posToUpdateCode);
										tableFile.writeByte(calculateSerialCodeAndSize(data_types.get(l))[0]);
										changed_rows++;
									} else if (updateValue.equals("null")) {
										tableFile.seek(posToUpdateCode);
										tableFile.writeByte(1);
										changed_rows++;
									}
								} else if (method.equals("int")) {
									if (!updateValue.equals("null")) {
										tableFile.seek(posToUpdateValue);
										tableFile.writeInt(Integer.parseInt(updateValue));
										tableFile.seek(posToUpdateCode);
										tableFile.writeByte(calculateSerialCodeAndSize(data_types.get(l))[0]);
										changed_rows++;
									} else if (updateValue.equals("null")) {
										tableFile.seek(posToUpdateCode);
										tableFile.writeByte(2);
										changed_rows++;
									}
								} else if (method.equals("long")) {
									if (!updateValue.equals("null")) {
										tableFile.seek(posToUpdateValue);
										tableFile.writeLong(Long.parseLong(updateValue));
										tableFile.seek(posToUpdateCode);
										tableFile.writeByte(calculateSerialCodeAndSize(data_types.get(l))[0]);
										changed_rows++;
									} else if (updateValue.equals("null")) {
										tableFile.seek(posToUpdateCode);
										tableFile.writeByte(3);
										changed_rows++;
									}
								} else if (method.equals("float")) {
									if (!updateValue.equals("null")) {
										tableFile.seek(posToUpdateValue);
										tableFile.writeFloat(Float.parseFloat(updateValue));
										tableFile.seek(posToUpdateCode);
										tableFile.writeByte(calculateSerialCodeAndSize(data_types.get(l))[0]);
										changed_rows++;
									} else if (updateValue.equals("null")) {
										tableFile.seek(posToUpdateCode);
										tableFile.writeByte(2);
										changed_rows++;
									}
								} else if (method.equals("double")) {
									if (!updateValue.equals("null")) {
										tableFile.seek(posToUpdateValue);
										double d = Double.parseDouble(updateValue);
										tableFile.writeDouble(d);
										tableFile.seek(posToUpdateCode);
										tableFile.writeByte(calculateSerialCodeAndSize(data_types.get(l))[0]);
										changed_rows++;
									} else if (updateValue.equals("null")) {
										tableFile.seek(posToUpdateValue);
										tableFile.writeDouble(0);
										tableFile.seek(posToUpdateCode);
										tableFile.writeByte(3);
										changed_rows++;
									}
								} else if (method.equals("line")) {
									tableFile.seek(posToUpdateValue);
									if (updateValue.equals("null\r")) {
										tableFile.writeBytes("\r");
										changed_rows++;
									} else {
										tableFile.writeBytes(updateValue);
										changed_rows++;
									}
								}
							}
						}
					}
				}
			}
			System.out.println(changed_rows + " row(s) affected");
			tableFile.close();
		} catch (Exception e) {
			System.out.println(ERROR_MESSAGE);
		}

	}

	public static void updateMetaTable(RandomAccessFile davisbaseTablesCatalog, String table_name, int value,
			boolean updateRootPage, boolean updateRecordCount) {
		try {
			int seek = 0;
			int right_child_pointer = 0;
			int page = 0;
			int records = 0;
			while (right_child_pointer != -1) {
				seek = (int) (page * pageSize);
				davisbaseTablesCatalog.seek(seek + 4);
				right_child_pointer = davisbaseTablesCatalog.readInt();
				page = right_child_pointer;
				davisbaseTablesCatalog.seek(seek + 1);
				records = davisbaseTablesCatalog.readByte();
				for (int i = 0; i < records; i++) {
					int posToRead = seek + 8 + (2 * i);
					davisbaseTablesCatalog.seek(posToRead);
					posToRead = davisbaseTablesCatalog.readShort();

					davisbaseTablesCatalog.seek(posToRead);
					int payload_size = davisbaseTablesCatalog.readShort();

					davisbaseTablesCatalog.seek(posToRead + 6);
					int columns = davisbaseTablesCatalog.readByte();

					davisbaseTablesCatalog.seek(posToRead + 6 + 1 + columns);
					String table_name_found = davisbaseTablesCatalog.readLine();

					if (table_name.equals(table_name_found)) {
						if (updateRootPage) {
							davisbaseTablesCatalog.seek((posToRead + payload_size + 6) - 3);
							davisbaseTablesCatalog.writeShort(value);
						}
						if (updateRecordCount) {
							davisbaseTablesCatalog.seek((posToRead + payload_size + 6) - 7);
							davisbaseTablesCatalog.writeInt(value);
						}

					}
				}
			}

		} catch (Exception e) {
			System.out.println(ERROR_MESSAGE);
		}
	}
}