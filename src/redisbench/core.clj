(ns redisbench.core
  (:gen-class)
  (:require [clojure.core.async :as async :refer [>! <! >!! <!! go go-loop thread chan close!]]
            [taoensso.timbre :as log]
            [taoensso.carmine :as car]
            [criterium.core :as criterium]
            [clojurewerkz.spyglass.client :as c]
            [clojure.java.io :as io :refer [file  input-stream]]))


;; redis stuff
(def server1-conn {:spec {:redis-host "localhost" :redis-port 6379}}) ; See `wcar` docstring for opts
(defmacro wcar* [& body] `(car/wcar server1-conn ~@body))

;;memcached stuff
;;(def tmc (c/bin-connection "127.0.0.1:11211"))


(defn uuid [] (str (java.util.UUID/randomUUID)))



(defn gen-keys
  "Create a vector of `n` keys, each keys being a `mult` UUIDs concatenated"
  [n mult]
  {:pre [(pos? mult)
         (pos? n)]}
  (letfn [(f []
            (apply str (take mult (repeatedly uuid))))]
    (vec (take n (repeatedly f)))))


(defn gen-payloads
  "Create a vector of `n` random chunks of data, each `len` bytes long"
  [n len]
  {:pre [(pos? len)
         (pos? n)]}
  (with-open [in (input-stream (file "/dev/urandom"))]
    (let [read-chunk #(let [buf (byte-array len)]
                        (.read in buf)
                        buf)]
      (vec (take n (repeatedly read-chunk))))))

(defn launch-writer
  [channel keys-source payloads-source add-counter-atom]
  (thread (while channel
        (let [k (rand-nth keys-source)
              p (rand-nth payloads-source)]
          (wcar* (car/set k p))
          ;(c/set tmc k 3600 p)
          (swap! add-counter-atom inc)
          (>!! channel k)))))

(defn launch-updater
  [channel keys-atom]
  (thread (loop []
            (let [k (<!! channel)]
              (swap! keys-atom conj k)
              (recur)))))

(defn launch-remover
  [keys-atom del-counter-atom continue?]
  (thread (do
        (Thread/sleep 1000)
        (while @continue?
          (let [k (rand-nth (vec @keys-atom))]
            (when k
              (do
                  ;(c/delete tmc k)
                  (wcar* (car/unlink k))
                  (swap! del-counter-atom inc)
                  (swap! keys-atom disj k)))
            (Thread/sleep 20))))))



(defn launch-logger
  [add-counter read-counter del-counter continue?]
  (thread
    (while @continue?
      (let [dbsize  #_(-> (c/get-stats tmc)
                       vals
                       (#(into {} %))
                       (get "total_items"))  (wcar* (car/dbsize))]
        (log/info (str "Added " @add-counter
                       ", read " @read-counter
                       ", deleted " @del-counter
                       " - " dbsize " keys in db"))
        (reset! add-counter 0)
        (reset! del-counter 0)
        (reset! read-counter 0)
        (Thread/sleep 1000)))))

(defn launch-reader
  [keys-source read-atom continue?]
  (thread (do 
            (Thread/sleep 500)
            (while @continue?
              (when-let [key (rand-nth keys-source)]
                (wcar* car/get key)
                ;; (c/get tmc key)
                (swap! read-atom inc))))))

(defn put-all-keys [ks vs]
  (doseq [k ks]
    (wcar* (car/set k (rand-nth vs)))))


(defn read-all-keys [ks]
  (doseq [k ks]
    (wcar* (car/get k))))


(defn delete-all-keys [ks]
  (doseq [k ks]
    (wcar* (car/unlink k))))


(defn -main
  [& args]
  (let [keys-source (gen-keys 500000 40)
        payloads-source (gen-payloads 1000 500)
        added-keys (atom #{})
        add-counter (atom 0)
        del-counter (atom 0)
        read-counter (atom 0)
        timer (atom 0)
        continue? (atom true)
        channel (chan 100)]
    (wcar* (car/flushall))
    (log/info "Starting...")
    (log/info (str "Inserting " (count keys-source) " keys with "
                   (count (first payloads-source)) " bytes each..."))
    (reset! timer (System/currentTimeMillis))
    (put-all-keys keys-source payloads-source)
    (let [time-consumed (- (System/currentTimeMillis) @timer)
          avg (float (/ time-consumed (count keys-source)))]
      (log/info (str "Inserts took " (double (/ time-consumed 1000)) "s (" avg "ms per insert)")))

    (log/info (str "Reading " (count keys-source) " keys with "
                   (count (first payloads-source)) " bytes each..."))
    (reset! timer (System/currentTimeMillis))
    (read-all-keys keys-source)
    (let [time-consumed (- (System/currentTimeMillis) @timer)
          avg (float (/ time-consumed (count keys-source)))]
      (log/info (str "Reads took " (double (/ time-consumed 1000)) "s (" avg "ms per read)")))
    (log/info "Starting read benchmark...")
    (criterium/with-progress-reporting
      (criterium/quick-bench (wcar* (car/get (rand-nth keys-source)))))
    (log/info "Starting write benchmark...")
    (criterium/with-progress-reporting
      (criterium/quick-bench (wcar* (car/set (rand-nth keys-source) (rand-nth payloads-source)))))

    (log/info (str "Deleting " (count keys-source) " keys with "
                   (count (first payloads-source)) " bytes each..."))
    (reset! timer (System/currentTimeMillis))
    (delete-all-keys keys-source)
    (let [time-consumed (- (System/currentTimeMillis) @timer)
          avg (float (/ time-consumed (count keys-source)))]
      (log/info (str "Deletes took " (double (/ time-consumed 1000)) "s (" avg "ms per delete)")))

    ;; (dotimes [n 4](launch-writer channel keys-source payloads-source add-counter))
    ;; (launch-updater channel added-keys)
    ;; (launch-remover added-keys del-counter continue?)
    ;; (dotimes [n 3] (launch-reader keys-source read-counter continue?))
    ;; (Thread/sleep 10000)
    ;; (launch-logger add-counter read-counter del-counter continue?)
    (Thread/sleep 300000)
    (shutdown-agents)
    (wcar* (car/flushall))))


