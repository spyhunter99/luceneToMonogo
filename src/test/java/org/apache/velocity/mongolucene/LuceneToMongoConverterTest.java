/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/UnitTests/JUnit5TestClass.java to edit this template
 */
package org.apache.velocity.mongolucene;

import com.mongodb.client.model.Filters;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.bson.conversions.Bson;
import org.junit.Test;

public class LuceneToMongoConverterTest {


    @Test
    public void testTermQuery() {
        Query query = new TermQuery(new Term("field1", "foo"));
        org.bson.conversions.Bson bson = LuceneToMongoConverter.convert(query);
        assertEquals(Filters.eq("field1", "foo"), bson);
    }

    @Test
    public void testPhraseQuery() {
        Query query = new PhraseQuery.Builder()
                .add(new Term("field1", "foo"))
                .add(new Term("field1", "bar"))
                .build();
        org.bson.conversions.Bson bson = LuceneToMongoConverter.convert(query);
        assertEquals(Filters.and(Filters.eq("field1", "foo"), Filters.eq("field1", "bar")), bson);
    }

    @Test
    public void testBooleanQuery() {
        Query query = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("field1", "foo")), BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("field2", "bar")), BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("field3", "baz")), BooleanClause.Occur.SHOULD)
                .build();
        org.bson.conversions.Bson bson = LuceneToMongoConverter.convert(query);
        assertEquals(Filters.and(Filters.eq("field1", "foo"), Filters.eq("field2", "bar"),
                Filters.or(Filters.eq("field3", "baz"), Filters.exists("field3"))), bson);
    }

    @Test
    public void testWildcardQuery() {
        Query query = new WildcardQuery(new Term("field1", "f*"));
        org.bson.conversions.Bson bson = LuceneToMongoConverter.convert(query);
        assertEquals(Filters.regex("field1", "f.*$"), bson);
    }

    //this is a passing test case....not sure why it's failing
    @Test
    public void testRangeQuery() {
        Query query = IntPoint.newRangeQuery("field1", 10, 20);
        org.bson.conversions.Bson bson = LuceneToMongoConverter.convert(query);
        assertEquals(Filters.and(Filters.gte("field1", 10), Filters.lte("field1", 20)), bson);
    }

    @Test
    public void testNumericRangeQuery() {
        Query query = LongPoint.newRangeQuery("field1", 100L, 200L);
        org.bson.conversions.Bson bson = LuceneToMongoConverter.convert(query);
        assertEquals(Filters.and(Filters.gte("field1", 100L), Filters.lte("field1", 200L)), bson);
    }

    @Test
    public void testMultiTermQuery() {
        //Query query = new MultiTermQueryWrapper<>(new PrefixQuery(new Term("field1", "foo")));
       // org.bson.conversions.Bson bson = converter.convert(query);
        //assertEquals(Filters.regex("field1", "^foo.*$"), bson);
    }

    @Test
    public void testConvertQuery() throws ParseException {

        String luceneQuery = "field1:foo AND field2:bar";
        QueryParser parser = new QueryParser(null, new StandardAnalyzer());
        Query query = parser.parse(luceneQuery);
        Bson bsonQuery = LuceneToMongoConverter.convert(query);

        Bson expectedBsonQuery = Filters.and(Filters.eq("field1", "foo"), Filters.eq("field2", "bar"));

        Assert.assertEquals(expectedBsonQuery, bsonQuery);
    }

    @Test
    public void testConvertQueryWithWildcard() throws ParseException {

        String luceneQuery = "field1:f*";
        QueryParser parser = new QueryParser(null, new StandardAnalyzer());
        Query query = parser.parse(luceneQuery);
        Bson bsonQuery = LuceneToMongoConverter.convert(query);

        Bson expectedBsonQuery = Filters.regex("field1", "f.*$");

        Assert.assertEquals(expectedBsonQuery, bsonQuery);
    }

    @Test
    public void testConvertQueryWithFuzzy() throws ParseException {

        String luceneQuery = "field1:foo~";
        QueryParser parser = new QueryParser(null, new StandardAnalyzer());
        Query query = parser.parse(luceneQuery);

        Bson bsonQuery = LuceneToMongoConverter.convert(query);

        Bson expectedBsonQuery = Filters.regex("field1", "foo.*");

        Assert.assertEquals(expectedBsonQuery, bsonQuery);
    }

    @Test
    public void testConvertQueryWithRange() throws ParseException {

        String luceneQuery = "field1:[0 TO 10]";
        QueryParser parser = new QueryParser(null, new StandardAnalyzer());

        Query query = parser.parse(luceneQuery);
        Bson bsonQuery = LuceneToMongoConverter.convert(query);

        Bson expectedBsonQuery = Filters.and(Filters.gte("field1", 0), Filters.lte("field1", 10));

        Assert.assertEquals(expectedBsonQuery, bsonQuery);
    }

    @Test
    public void testConvertQueryWithBooleanAndRange() throws ParseException {

        String luceneQuery = "(field1:[0 TO 10] OR field2:bar) AND field3:baz";
        QueryParser parser = new QueryParser(null, new StandardAnalyzer());
        Query query = parser.parse(luceneQuery);
        Bson bsonQuery = LuceneToMongoConverter.convert(query);

        Bson expectedBsonQuery = Filters.and(
                Filters.or(Filters.and(Filters.gte("field1", 0), Filters.lte("field1", 10)), Filters.eq("field2", "bar")),
                Filters.eq("field3", "baz"));

        Assert.assertEquals(expectedBsonQuery, bsonQuery);
    }

    @Test
    public void testConvertQueryWithNegation() throws ParseException {

        String luceneQuery = "NOT field1:foo";
        QueryParser parser = new QueryParser(null, new StandardAnalyzer());
        Query query = parser.parse(luceneQuery);
        Bson bsonQuery = LuceneToMongoConverter.convert(query);

        Bson expectedBsonQuery = Filters.not(Filters.eq("field1", "foo"));

        Assert.assertEquals(expectedBsonQuery, bsonQuery);
    }

    @Test
    public void testConvertQueryWithMultipleTerms() throws ParseException {

        String luceneQuery = "field1:foo field2:bar";
        QueryParser parser = new QueryParser(null, new StandardAnalyzer());
        Query query = parser.parse(luceneQuery);
        Bson bsonQuery = LuceneToMongoConverter.convert(query);

        Bson expectedBsonQuery = Filters.and(Filters.eq("field1", "foo"), Filters.eq("field2", "bar"));

        Assert.assertEquals(expectedBsonQuery, bsonQuery);
    }

    @Test
    public void testConvertQuery22() {
        // Single term query
        Query termQuery = new TermQuery(new Term("field1", "foo"));
        Bson expectedTermFilter = Filters.eq("field1", "foo");
        Assert.assertEquals(expectedTermFilter, LuceneToMongoConverter.convert(termQuery));

        // Boolean query with multiple clauses
        BooleanQuery booleanQuery = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("field1", "foo")), BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("field2", "bar")), BooleanClause.Occur.MUST_NOT)
                .add(new TermQuery(new Term("field3", "baz")), BooleanClause.Occur.SHOULD)
                .build();
        Bson expectedBooleanFilter = Filters.and(
                Filters.eq("field1", "foo"),
                Filters.not(Filters.eq("field2", "bar")),
                Filters.eq("field3", "baz")
        );
        Assert.assertEquals(expectedBooleanFilter, LuceneToMongoConverter.convert(booleanQuery));
    }
}
