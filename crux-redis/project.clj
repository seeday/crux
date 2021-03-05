(defproject juxt/crux-redis "crux-git-version-alpha"
  :description "Crux Redis"
  :url "https://github.com/juxt/crux"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "1.1.0"]
                 [juxt/crux-core "crux-git-version-beta"]
                 [com.taoensso/carmine "3.1.0"]
                 [com.taoensso/nippy "3.1.1"]
                 [pool "0.2.1"]
                 [io.lettuce/lettuce-core "6.0.2.RELEASE"]]
  :test-dependencies [[criterium "0.4.6"]]

  :middleware [leiningen.project-version/middleware]
  :pedantic? :warn)
