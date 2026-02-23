package service

import com.github.pemistahl.lingua.api.Language
import com.github.pemistahl.lingua.api.LanguageDetectorBuilder
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.ar.ArabicAnalyzer
import org.apache.lucene.analysis.bg.BulgarianAnalyzer
import org.apache.lucene.analysis.bn.BengaliAnalyzer
import org.apache.lucene.analysis.ca.CatalanAnalyzer
import org.apache.lucene.analysis.cjk.CJKAnalyzer
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer
import org.apache.lucene.analysis.cz.CzechAnalyzer
import org.apache.lucene.analysis.da.DanishAnalyzer
import org.apache.lucene.analysis.de.GermanAnalyzer
import org.apache.lucene.analysis.el.GreekAnalyzer
import org.apache.lucene.analysis.core.LowerCaseFilter
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.en.EnglishPossessiveFilter
import org.apache.lucene.analysis.en.KStemFilter
import org.apache.lucene.analysis.standard.StandardTokenizer
import org.apache.lucene.analysis.es.SpanishAnalyzer
import org.apache.lucene.analysis.et.EstonianAnalyzer
import org.apache.lucene.analysis.eu.BasqueAnalyzer
import org.apache.lucene.analysis.fa.PersianAnalyzer
import org.apache.lucene.analysis.fi.FinnishAnalyzer
import org.apache.lucene.analysis.fr.FrenchAnalyzer
import org.apache.lucene.analysis.ga.IrishAnalyzer
import org.apache.lucene.analysis.gl.GalicianAnalyzer
import org.apache.lucene.analysis.hi.HindiAnalyzer
import org.apache.lucene.analysis.hu.HungarianAnalyzer
import org.apache.lucene.analysis.hy.ArmenianAnalyzer
import org.apache.lucene.analysis.id.IndonesianAnalyzer
import org.apache.lucene.analysis.it.ItalianAnalyzer
import org.apache.lucene.analysis.ja.JapaneseAnalyzer
import org.apache.lucene.analysis.lt.LithuanianAnalyzer
import org.apache.lucene.analysis.lv.LatvianAnalyzer
import org.apache.lucene.analysis.nl.DutchAnalyzer
import org.apache.lucene.analysis.no.NorwegianAnalyzer
import org.apache.lucene.analysis.pt.PortugueseAnalyzer
import org.apache.lucene.analysis.ro.RomanianAnalyzer
import org.apache.lucene.analysis.ru.RussianAnalyzer
import org.apache.lucene.analysis.sr.SerbianAnalyzer
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.sv.SwedishAnalyzer
import org.apache.lucene.analysis.ta.TamilAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.tr.TurkishAnalyzer
import kotlin.jvm.java

// @author ChatGPT
private class KStemEnglishAnalyzer : Analyzer() {
    override fun createComponents(fieldName: String): TokenStreamComponents {
        val tokenizer = StandardTokenizer()

        val stream = KStemFilter(
            org.apache.lucene.analysis.StopFilter(
                LowerCaseFilter(
                    EnglishPossessiveFilter(tokenizer)
                ),
                EnglishAnalyzer.getDefaultStopSet()
            )
        )

        return TokenStreamComponents(tokenizer, stream)
    }
}

object StemService {

    private val detector by lazy {
        LanguageDetectorBuilder
            .fromAllLanguages()
            .withPreloadedLanguageModels()
            .build()
    }

    // @author ChatGPT
    private val analyzers: Map<String, Analyzer> = mapOf(
        "ARABIC" to ArabicAnalyzer(),
        "ARMENIAN" to ArmenianAnalyzer(),
        "BASQUE" to BasqueAnalyzer(),
        "BENGALI" to BengaliAnalyzer(),
        "BULGARIAN" to BulgarianAnalyzer(),
        "CATALAN" to CatalanAnalyzer(),
        "CZECH" to CzechAnalyzer(),
        "DANISH" to DanishAnalyzer(),
        "DUTCH" to DutchAnalyzer(),
        "ENGLISH" to KStemEnglishAnalyzer(),
        "ESTONIAN" to EstonianAnalyzer(),
        "FINNISH" to FinnishAnalyzer(),
        "FRENCH" to FrenchAnalyzer(),
        "GALICIAN" to GalicianAnalyzer(),
        "GERMAN" to GermanAnalyzer(),
        "GREEK" to GreekAnalyzer(),
        "HINDI" to HindiAnalyzer(),
        "HUNGARIAN" to HungarianAnalyzer(),
        "INDONESIAN" to IndonesianAnalyzer(),
        "IRISH" to IrishAnalyzer(),
        "ITALIAN" to ItalianAnalyzer(),
        "LATVIAN" to LatvianAnalyzer(),
        "LITHUANIAN" to LithuanianAnalyzer(),
        "BOKMAL" to NorwegianAnalyzer(),
        "NYNORSK" to NorwegianAnalyzer(),
        "PERSIAN" to PersianAnalyzer(),
        "PORTUGUESE" to PortugueseAnalyzer(),
        "ROMANIAN" to RomanianAnalyzer(),
        "RUSSIAN" to RussianAnalyzer(),
        "SERBIAN" to SerbianAnalyzer(),
        "SPANISH" to SpanishAnalyzer(),
        "SWEDISH" to SwedishAnalyzer(),
        "TAMIL" to TamilAnalyzer(),
        "TURKISH" to TurkishAnalyzer(),
        "JAPANESE" to JapaneseAnalyzer(),
        "CHINESE" to SmartChineseAnalyzer(),
        "KOREAN" to CJKAnalyzer()
    )

    private val fallback = StandardAnalyzer()

    fun detectLanguage(text: String): LanguageResult? {
        val result = detector.computeLanguageConfidenceValues(text)
            .maxByOrNull { it.value }
            ?: return null

        if (result.key == Language.UNKNOWN)
            return null

        return LanguageResult(
            language = result.key.name,
            confidence = result.value
        )
    }

    fun processText(text: String, language: String?): String {
        val analyzer = language.let { analyzers[it] } ?: fallback
        val tokenStream = analyzer.tokenStream("text", text)
        val charTermAttr = tokenStream.addAttribute(CharTermAttribute::class.java)
        tokenStream.reset()

        val tokens = buildList {
            while (tokenStream.incrementToken()) {
                add(charTermAttr.toString())
            }
        }

        tokenStream.end()
        tokenStream.close()

        return tokens.joinToString(" ") { it.replace(" ", "") }
    }

    data class LanguageResult(
        val language: String,
        val confidence: Double
    )

}
