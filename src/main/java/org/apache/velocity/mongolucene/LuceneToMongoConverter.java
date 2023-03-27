/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */
package org.apache.velocity.mongolucene;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;

import com.mongodb.client.model.Filters;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.NumericUtils;
import org.bson.conversions.Bson;

public class LuceneToMongoConverter {

    public static org.bson.conversions.Bson convert(Query luceneQuery) {
        if (luceneQuery instanceof BooleanQuery) {
            BooleanQuery booleanQuery = (BooleanQuery) luceneQuery;
            return convertBooleanQuery(booleanQuery);
        } else if (luceneQuery instanceof TermQuery) {
            TermQuery termQuery = (TermQuery) luceneQuery;
            return convertTermQuery(termQuery);
        } else if (luceneQuery instanceof WildcardQuery) {
            WildcardQuery wildcardQuery = (WildcardQuery) luceneQuery;
            return convertWildcardQuery(wildcardQuery);
        } else if (luceneQuery instanceof PointRangeQuery) {
            PointRangeQuery longPoint = (PointRangeQuery) luceneQuery;
            long lower = Long.MIN_VALUE;
            long upper = Long.MAX_VALUE;

            if (longPoint.getLowerPoint().length == Long.BYTES) {
                lower = NumericUtils.sortableBytesToLong(longPoint.getLowerPoint(), 0);
            }
            if (longPoint.getLowerPoint().length == Integer.BYTES) {
                lower = NumericUtils.sortableBytesToInt(longPoint.getLowerPoint(), 0);
            }
            if (longPoint.getUpperPoint().length == Long.BYTES) {
                upper = NumericUtils.sortableBytesToLong(longPoint.getUpperPoint(), 0);
            }
            if (longPoint.getUpperPoint().length == Integer.BYTES) {
                upper = NumericUtils.sortableBytesToInt(longPoint.getUpperPoint(), 0);
            }
            return Filters.and(
                    Filters.gte(longPoint.getField(), lower),
                    Filters.lte(longPoint.getField(), upper)
            );
        } else if (luceneQuery instanceof TermRangeQuery) {
            TermRangeQuery termRangeQuery = (TermRangeQuery) luceneQuery;
            return Filters.and(
                    Filters.gte(termRangeQuery.getField(),
                            termRangeQuery.getLowerTerm() != null ? termRangeQuery.getLowerTerm().utf8ToString() : null),
                    Filters.lte(termRangeQuery.getField(),
                            termRangeQuery.getUpperTerm() != null ? termRangeQuery.getUpperTerm().utf8ToString() : null)
            );
        } else if (luceneQuery instanceof PrefixQuery) {
            PrefixQuery prefixQuery = (PrefixQuery) luceneQuery;
            return convertPrefixQuery(prefixQuery);
        } else if (luceneQuery instanceof RegexpQuery) {
            RegexpQuery regexpQuery = (RegexpQuery) luceneQuery;
            return convertRegexpQuery(regexpQuery);
        } else if (luceneQuery instanceof FuzzyQuery) {
            FuzzyQuery fuzzyQuery = (FuzzyQuery) luceneQuery;
            return convertFuzzyQuery(fuzzyQuery);
        } else if (luceneQuery instanceof MatchAllDocsQuery) {
            return Filters.exists("_id");
        } else if (luceneQuery instanceof PhraseQuery) {
            /*Term[] terms = ((PhraseQuery) luceneQuery).getTerms();
            Bson[] filters = new Bson[terms.length];
            for (int i = 0; i < terms.length; i++) {
                filters[i] = Filters.eq(terms[i].field(), terms[i].text());
            }
            return Filters.and(filters);*/
            PhraseQuery phraseQuery = (PhraseQuery) luceneQuery;
            Term[] terms = phraseQuery.getTerms();
            return Filters.all(Arrays.stream(terms).map(t -> {
                String[] parts = t.field().split("\\.");
                Bson bson = Filters.eq(parts[parts.length - 1], t.text());
                for (int i = parts.length - 2; i >= 0; i--) {
                    bson = Filters.eq(parts[i], bson);
                }
                return bson;
            }).collect(Collectors.toList()));

        } else {
            throw new IllegalArgumentException("Unsupported Lucene query type: " + luceneQuery.getClass().getName());
        }
    }

    private static org.bson.conversions.Bson convertBooleanQuery(BooleanQuery booleanQuery) {
        List<Bson> clauses = new ArrayList<>();
        for (BooleanClause clause : booleanQuery) {
            Query query = clause.getQuery();
            if (query instanceof BooleanQuery) {
                clauses.add(convertBooleanQuery((BooleanQuery) query, clause.getOccur()));
            } else {
                clauses.add(convertQuery(query, clause.getOccur()));
            }
        }
        return Filters.and(clauses);
    }

    private static org.bson.conversions.Bson convertBooleanQuery(BooleanQuery booleanQuery, Occur occur) {
        List<Bson> clauses = new ArrayList<>();
        for (BooleanClause clause : booleanQuery) {
            Query query = clause.getQuery();
            if (query instanceof BooleanQuery) {
                clauses.add(convertBooleanQuery((BooleanQuery) query, clause.getOccur()));
            } else {
                clauses.add(convertQuery(query, occur));
            }
        }
        return Filters.or(clauses);
    }

    private static org.bson.conversions.Bson convertQuery(Query query, Occur occur) {
        if (query instanceof TermQuery) {
            Term term = ((TermQuery) query).getTerm();
            return occur == Occur.MUST
                    ? Filters.eq(term.field(), term.text())
                    : Filters.ne(term.field(), term.text());
        } else if (query instanceof PointRangeQuery) {
            PointRangeQuery longPoint = (PointRangeQuery) query;
            long lower = Long.MIN_VALUE;
            long upper = Long.MAX_VALUE;

            if (longPoint.getLowerPoint().length == Long.BYTES) {
                lower = NumericUtils.sortableBytesToLong(longPoint.getLowerPoint(), 0);
            }
            if (longPoint.getLowerPoint().length == Integer.BYTES) {
                lower = NumericUtils.sortableBytesToInt(longPoint.getLowerPoint(), 0);
            }
            if (longPoint.getUpperPoint().length == Long.BYTES) {
                upper = NumericUtils.sortableBytesToLong(longPoint.getUpperPoint(), 0);
            }
            if (longPoint.getUpperPoint().length == Integer.BYTES) {
                upper = NumericUtils.sortableBytesToInt(longPoint.getUpperPoint(), 0);
            }
            return Filters.and(
                    Filters.gte(longPoint.getField(), lower),
                    Filters.lte(longPoint.getField(), upper)
            );
        } else if (query instanceof PhraseQuery) {
            Term[] terms = ((PhraseQuery) query).getTerms();
            Bson[] filters = new Bson[terms.length];
            for (int i = 0; i < terms.length; i++) {
                filters[i] = occur == Occur.MUST
                        ? Filters.eq(terms[i].field(), terms[i].text())
                        : Filters.ne(terms[i].field(), terms[i].text());
            }
            return Filters.and(filters);
        } else if (query instanceof TermRangeQuery) {
            TermRangeQuery termRangeQuery = (TermRangeQuery) query;
            return Filters.and(
                    Filters.gte(termRangeQuery.getField(),
                            termRangeQuery.getLowerTerm() != null ? termRangeQuery.getLowerTerm().utf8ToString() : null),
                    Filters.lte(termRangeQuery.getField(),
                            termRangeQuery.getUpperTerm() != null ? termRangeQuery.getUpperTerm().utf8ToString() : null)
            );
        } else if (query instanceof BooleanQuery) {
            return convertBooleanQuery((BooleanQuery) query, occur);
        } else {
            throw new IllegalArgumentException("Unsupported query type: " + query.getClass());
        }
    }

    private static org.bson.conversions.Bson convertTermQuery(TermQuery termQuery) {
        Term term = termQuery.getTerm();
        return Filters.eq(term.field(), term.text());
    }

    private static org.bson.conversions.Bson convertWildcardQuery(WildcardQuery wildcardQuery) {
        Term term = wildcardQuery.getTerm();
        String pattern = wildcardToRegex(term.text());
        return Filters.regex(term.field(), pattern);
    }

    private static org.bson.conversions.Bson convertPrefixQuery(PrefixQuery prefixQuery) {
        Term term = prefixQuery.getPrefix();
        String pattern = wildcardToRegex(term.text() + "*");
        return Filters.regex(term.field(), pattern);
    }

    private static org.bson.conversions.Bson convertRegexpQuery(RegexpQuery regexpQuery) {
        Term term = regexpQuery.getRegexp();
        return Filters.regex(term.field(), term.text());
    }

    private static org.bson.conversions.Bson convertFuzzyQuery(FuzzyQuery fuzzyQuery) {
        Term term = fuzzyQuery.getTerm();
        return Filters.regex(term.field(), fuzzyToRegex(term.text(), fuzzyQuery.getMaxEdits()));
    }

    private static String wildcardToRegex(String wildcard) {
        StringBuilder regex = new StringBuilder();
        for (int i = 0; i < wildcard.length(); i++) {
            char c = wildcard.charAt(i);
            switch (c) {
                case '*':
                    regex.append(".*");
                    break;
                case '?':
                    regex.append(".");
                    break;
                default:
                    regex.append(c);
            }
        }
        return regex.toString();
    }

    private static String fuzzyToRegex(String fuzzy, int maxEdits) {
        StringBuilder regex = new StringBuilder();
        regex.append("(");
        for (int i = 0; i < fuzzy.length(); i++) {
            char c = fuzzy.charAt(i);
            if (i < maxEdits) {
                regex.append("[").append(c).append("]");
            } else {
                regex.append(c);
            }
        }
        regex.append("){s<=").append(maxEdits).append("}");
        return regex.toString();
    }
}
