(defproject redisbench "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [com.taoensso/carmine "2.19.1"]
                 [com.taoensso/timbre "4.10.0"]
                 [org.clojure/core.async "0.4.474"]
                 [clojurewerkz/spyglass "1.2.0"]
                 [criterium "0.4.4"]
                 ]
  :main ^:skip-aot redisbench.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
