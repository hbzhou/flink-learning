package com.itsz.flink.kafka.boot.rtq;

public enum RtqType {

	STOCKSNAPSHOT("110"),
	STOCKSTATUS("115"),
	BROKERQUENE("130"),
	INDEX("210"),
	DELAY_INDEX("211"),
	STOCK_CONNECT_MARKET_TURNOVER("435"),
	STOCK_CONNECT_DAILY_QUOTA_BALANCE("430"),
	WORLD_INDEX("710"),
	;



	private String tableId;

	private RtqType(String tableId) {
		this.tableId = tableId;
	}

	public static String getType(String tableId) {
		switch (tableId) {
		case "110":
			return STOCKSNAPSHOT.name();

		default:
			return null;
		}
	}

	public String getTableId() {
		return tableId;
	}

	public void setTableId(String tableId) {
		this.tableId = tableId;
	}

}
