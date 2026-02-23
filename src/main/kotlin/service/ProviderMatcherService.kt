package dev.qr.services

import com.gliwka.hyperscan.wrapper.Database
import com.gliwka.hyperscan.wrapper.Expression
import com.gliwka.hyperscan.wrapper.ExpressionFlag
import com.gliwka.hyperscan.wrapper.Scanner
import dev.qr.util.BalancerMutex
import dev.qr.util.BalancerSupplier
import kotlinx.coroutines.coroutineScope
import java.util.EnumSet

object ProviderMatcherService {

    private enum class Kind { Pattern, Exception }
    private data class Meta(val idx: Int, val kind: Kind)

    private val metaByExprId = ArrayList<Meta>()

    private val db: Database
    private val scanner: BalancerMutex<String, List<UrlService.Pattern>>

    private val specialPattern = UrlService.patterns.single { it.urlPattern == ".*" }

    init {
        val rules = UrlService.patterns
        val flags = EnumSet.of(ExpressionFlag.SINGLEMATCH, ExpressionFlag.UTF8)

        val expressions = ArrayList<Expression>()
        var nextId = 0

        fun registerExpression(providerIndex: Int, kind: Kind, pattern: String) {
            val expression = Expression(pattern, flags, nextId)

            val validation = expression.validate()
            require(validation.isValid) {
                "Vectorscan cannot compile pattern for provider=${rules[providerIndex]} kind=$kind: ${validation.errorMessage}"
            }

            expressions.add(expression)
            metaByExprId.add(Meta(providerIndex, kind))
            require(nextId == metaByExprId.size - 1)

            nextId++
        }

        for ((idx, pattern) in rules.withIndex()) {
            if (pattern.urlPattern == ".*") continue

            registerExpression(idx, Kind.Pattern, pattern.urlPattern)

            for (ex in pattern.exceptions) {
                registerExpression(idx, Kind.Exception, ex)
            }
        }

        db = Database.compile(expressions)
        scanner = BalancerMutex(50, object : BalancerSupplier<String, List<UrlService.Pattern>> {
            private val scanners = Array(50) {
                Scanner().also { it.allocScratch(db) }
            }

            override suspend fun supply(input: String, thread: Int): List<UrlService.Pattern> {
                val included = BooleanArray(rules.size)
                val excluded = BooleanArray(rules.size)

                // Callback-based scan avoids allocating Match objects for each hit. :contentReference[oaicite:6]{index=6}
                scanners[thread].scan(db, input) { expression, _, _ ->
                    val meta = metaByExprId[expression.id]
                    when (meta.kind) {
                        Kind.Pattern -> included[meta.idx] = true
                        Kind.Exception -> excluded[meta.idx] = true
                    }
                    true
                }

                val out = ArrayList<UrlService.Pattern>()

                out.add(specialPattern)

                for (i in rules.indices) {
                    if (included[i] && !excluded[i]) out.add(rules[i])
                }

                return out
            }
        })
    }

    suspend fun relevantPatterns(url: String): List<UrlService.Pattern> = coroutineScope { scanner.eval(url) }

}