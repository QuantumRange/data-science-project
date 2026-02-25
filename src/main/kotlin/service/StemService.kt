package service

import com.github.jfasttext.JFastText
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.analysis.ar.ArabicAnalyzer
import org.apache.lucene.analysis.bg.BulgarianAnalyzer
import org.apache.lucene.analysis.bn.BengaliAnalyzer
import org.apache.lucene.analysis.ca.CatalanAnalyzer
import org.apache.lucene.analysis.cjk.CJKAnalyzer
import org.apache.lucene.analysis.cn.smart.SmartChineseAnalyzer
import org.apache.lucene.analysis.core.LowerCaseFilter
import org.apache.lucene.analysis.cz.CzechAnalyzer
import org.apache.lucene.analysis.da.DanishAnalyzer
import org.apache.lucene.analysis.de.GermanAnalyzer
import org.apache.lucene.analysis.el.GreekAnalyzer
import org.apache.lucene.analysis.en.EnglishAnalyzer
import org.apache.lucene.analysis.en.EnglishPossessiveFilter
import org.apache.lucene.analysis.en.KStemFilter
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
import org.apache.lucene.analysis.standard.StandardTokenizer
import org.apache.lucene.analysis.sv.SwedishAnalyzer
import org.apache.lucene.analysis.ta.TamilAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.tr.TurkishAnalyzer
import kotlin.math.exp

object StemService {

    private val detector by lazy {
        JFastText().apply {
            loadModel("/mnt/Fast2T/kotlin/2026/data-science-project/lid.176.ftz")
        }
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
        "ENGLISH" to KStemEnglishAnalyzer,
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
        val sample = text.take(3000).replace("\n", " ")
        val prediction: JFastText.ProbLabel = detector.predictProba(sample) ?: return null

        val label = prediction.label
            .removePrefix("__label__")
            .uppercase()

        return LanguageResult(
            language = isoToLanguage[label] ?: label,
            confidence = exp(prediction.logProb.toDouble())
        )
    }

    // @author ChatGPT
    private val isoToLanguage = mapOf(
        "AR" to "ARABIC",
        "HY" to "ARMENIAN",
        "EU" to "BASQUE",
        "BN" to "BENGALI",
        "BG" to "BULGARIAN",
        "CA" to "CATALAN",
        "CS" to "CZECH",
        "DA" to "DANISH",
        "NL" to "DUTCH",
        "EN" to "ENGLISH",
        "ET" to "ESTONIAN",
        "FI" to "FINNISH",
        "FR" to "FRENCH",
        "GL" to "GALICIAN",
        "DE" to "GERMAN",
        "EL" to "GREEK",
        "HI" to "HINDI",
        "HU" to "HUNGARIAN",
        "ID" to "INDONESIAN",
        "GA" to "IRISH",
        "IT" to "ITALIAN",
        "LV" to "LATVIAN",
        "LT" to "LITHUANIAN",
        "NO" to "BOKMAL",
        "NB" to "BOKMAL",
        "NN" to "NYNORSK",
        "FA" to "PERSIAN",
        "PT" to "PORTUGUESE",
        "RO" to "ROMANIAN",
        "RU" to "RUSSIAN",
        "SR" to "SERBIAN",
        "ES" to "SPANISH",
        "SV" to "SWEDISH",
        "TA" to "TAMIL",
        "TR" to "TURKISH",
        "JA" to "JAPANESE",
        "ZH" to "CHINESE",
        "KO" to "KOREAN"
    )

    private val specialCharacters = ".,+*#!\"§$%&/()=?0123456789*+<>^°\n[]{}".toSet()

    fun processText(text: String, language: String?): String {
        val analyzer = language.let { analyzers[it] } ?: fallback

        val tokenStream = analyzer.tokenStream("text", text)
        val charTermAttr = tokenStream.addAttribute(CharTermAttribute::class.java)
        tokenStream.reset()

        val tokens = buildList {
            while (tokenStream.incrementToken()) {
                val word = charTermAttr.toString()

                if (word.none { it in specialCharacters }) {
                    add(word)
                }
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

    // @author ChatGPT
    private object KStemEnglishAnalyzer : Analyzer() {
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

}
