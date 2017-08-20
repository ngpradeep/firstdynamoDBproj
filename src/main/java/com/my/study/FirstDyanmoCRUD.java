package com.my.study;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.sun.org.apache.xml.internal.dtm.ref.DTMDefaultBaseIterators.DescendantIterator;

public class FirstDyanmoCRUD {

	private static AmazonDynamoDB amazonDynamoDB;

	private static DynamoDB dynamoDB;

	private static String tableName = "Person";

	public static void main(String[] args) {
		try {
			
			initConfig();
			System.out.println("started the main");
			Table personTab = createDynamoDBTable();
			updateDynamoDbTable();
			loadDynamodata();
		//	deleteDynamoDBTable();
		//	TableDescription tableDescription =personTab.getDescription();
			//System.out.println("table Status ---->"+tableDescription.getTableStatus());
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void initConfig() throws Exception {

		amazonDynamoDB = AmazonDynamoDBClientBuilder.standard()
				.withEndpointConfiguration(new EndpointConfiguration("http://localhost:8000", "us-west-2")).build();
		dynamoDB = new DynamoDB(amazonDynamoDB);

	}

	public static Table createDynamoDBTable() throws Exception {
		System.out.println("started the main");
		List<AttributeDefinition> attributeDefination = new ArrayList<AttributeDefinition>();
		attributeDefination.add(new AttributeDefinition().withAttributeName("PersonId").withAttributeType("N"));
		attributeDefination.add(new AttributeDefinition().withAttributeName("PersonName").withAttributeType("S"));
		List<KeySchemaElement> keySchemaElements = new ArrayList<KeySchemaElement>();
		keySchemaElements.add(new KeySchemaElement().withAttributeName("PersonId").withKeyType("HASH"));
		keySchemaElements.add(new KeySchemaElement().withAttributeName("PersonName").withKeyType("RANGE"));

		CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
				.withAttributeDefinitions(attributeDefination).withKeySchema(keySchemaElements)
				.withProvisionedThroughput(
						new ProvisionedThroughput().withReadCapacityUnits(10L).withWriteCapacityUnits(5L));
		boolean flag =TableUtils.createTableIfNotExists(amazonDynamoDB, createTableRequest);
		TableUtils.waitUntilActive(amazonDynamoDB, tableName);
		Table personTable = null ;
		if (flag == true ){
			personTable= dynamoDB.getTable(tableName);
		}
		
		return personTable;
	}
	
	public static void updateDynamoDbTable() throws Exception {
		System.out.println("----->inside updatetable");
		Table personUpdatetable = dynamoDB.getTable(tableName);
		ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L);
		personUpdatetable.updateTable(provisionedThroughput);
		TableUtils.waitUntilActive(amazonDynamoDB, tableName);
		
	}
	
	public static void deleteDynamoDBTable() throws Exception{
		Table deleteTable = dynamoDB.getTable(tableName);
		deleteTable.delete();
		deleteTable.waitForDelete();	
	}
	
	public static void loadDynamodata() throws Exception {
		
		Table loadDataTable = dynamoDB.getTable(tableName);
		Item item = new Item().withPrimaryKey("PersonId", 56534, "PersonName", "Pradeep Kumar").withString("emailId", "ngpradeep.kumar@hotmail.com").withNumber("phone", 4144395915L);
		loadDataTable.putItem(item);
		
	}
}
	
	