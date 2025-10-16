// App.jsx - Real-time Firestore Integration
import React, { useEffect, useState } from "react";
import { collection, query, orderBy, onSnapshot } from "firebase/firestore";
import { db } from "./firebase";
import { motion, AnimatePresence } from "framer-motion";
import { Trophy, Loader2, Clock, CheckCircle, AlertCircle, Activity } from "lucide-react";

const Leaderboard = ({ participants, lastUpdate, isLive }) => {
  const sorted = [...participants].sort((a, b) => b.completed_count - a.completed_count);

  if (sorted.length === 0) {
    return (
      <div className="w-full max-w-4xl bg-gray-800/40 rounded-2xl p-8 text-center">
        <AlertCircle className="w-12 h-12 mx-auto text-yellow-500 mb-4" />
        <p className="text-gray-400">No participants found. Waiting for data...</p>
      </div>
    );
  }

  return (
    <div className="w-full max-w-4xl">
      {/* Stats Bar */}
      <div className="grid grid-cols-3 gap-4 mb-6">
        <div className="bg-gradient-to-br from-purple-900/40 to-purple-800/40 rounded-xl p-4 border border-purple-700/50">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-gray-400 text-sm">Total Participants</p>
              <p className="text-2xl font-bold text-white">{sorted.length}</p>
            </div>
            <Activity className="w-8 h-8 text-purple-400" />
          </div>
        </div>
        
        <div className="bg-gradient-to-br from-pink-900/40 to-pink-800/40 rounded-xl p-4 border border-pink-700/50">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-gray-400 text-sm">Top Score</p>
              <p className="text-2xl font-bold text-white">{sorted[0]?.completed_count || 0}/20</p>
            </div>
            <Trophy className="w-8 h-8 text-pink-400" />
          </div>
        </div>
        
        <div className="bg-gradient-to-br from-blue-900/40 to-blue-800/40 rounded-xl p-4 border border-blue-700/50">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-gray-400 text-sm">Status</p>
              <p className="text-sm font-semibold text-white flex items-center gap-1">
                {isLive ? (
                  <>
                    <span className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></span>
                    Live
                  </>
                ) : (
                  <>
                    <span className="w-2 h-2 bg-gray-500 rounded-full"></span>
                    Connecting...
                  </>
                )}
              </p>
            </div>
            <Clock className="w-8 h-8 text-blue-400" />
          </div>
        </div>
      </div>

      {/* Leaderboard */}
      <div className="bg-gray-800/40 rounded-2xl p-6 shadow-lg border border-gray-700">
        <AnimatePresence mode="popLayout">
          {sorted.map((user, index) => {
            const isTop3 = index < 3;
            const borderColor = isTop3 
              ? index === 0 ? 'border-yellow-500/50' 
              : index === 1 ? 'border-gray-400/50' 
              : 'border-orange-600/50'
              : 'border-gray-700/50';
            
            return (
              <motion.div
                key={user.userId}
                layout
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, scale: 0.9 }}
                transition={{ duration: 0.3, layout: { duration: 0.3 } }}
                className={`flex items-center justify-between bg-gray-900/70 hover:bg-gray-800/80 
                           p-4 rounded-xl mb-3 border ${borderColor} transition-all`}
              >
                <div className="flex items-center space-x-4 flex-1">
                  {/* Rank */}
                  <div className={`text-xl font-bold w-10 text-center ${
                    isTop3 
                      ? index === 0 ? 'text-yellow-400' 
                      : index === 1 ? 'text-gray-300' 
                      : 'text-orange-400'
                      : 'text-gray-400'
                  }`}>
                    {index === 0 ? '🥇' : index === 1 ? '🥈' : index === 2 ? '🥉' : index + 1}
                  </div>
                  
                  {/* Avatar */}
                  <img
                    src={user.profilePic || `https://api.dicebear.com/7.x/avataaars/svg?seed=${user.name}`}
                    alt={user.name}
                    className="w-14 h-14 rounded-full border-2 border-gray-600 object-cover"
                  />
                  
                  {/* User Info */}
                  <div className="flex-1">
                    <h2 className="text-lg font-semibold text-white flex items-center gap-2">
                      {user.displayName || user.name}
                      {user.completed_count === 20 && (
                        <CheckCircle className="w-5 h-5 text-green-400" />
                      )}
                    </h2>
                    <p className="text-sm text-gray-400">{user.email || 'No email'}</p>
                    {user.error && (
                      <p className="text-xs text-red-400 mt-1">⚠️ Error fetching data</p>
                    )}
                  </div>
                </div>
                
                {/* Score */}
                <div className="flex flex-col items-end">
                  <div className="flex items-center space-x-2 text-pink-400 font-bold text-xl">
                    <Trophy className="w-5 h-5" />
                    <span>{user.completed_count || 0}/20</span>
                  </div>
                  
                  {/* Progress Bar */}
                  <div className="w-32 bg-gray-700 rounded-full h-2 mt-2">
                    <motion.div
                      initial={{ width: 0 }}
                      animate={{ width: `${(user.completed_count / 20) * 100}%` }}
                      transition={{ duration: 0.5, delay: 0.1 }}
                      className="bg-gradient-to-r from-pink-500 to-purple-500 h-2 rounded-full"
                    />
                  </div>
                </div>
              </motion.div>
            );
          })}
        </AnimatePresence>
      </div>
      
      {/* Last Update */}
      {lastUpdate && (
        <div className="mt-4 text-center text-sm text-gray-500">
          Last updated: {lastUpdate}
        </div>
      )}
    </div>
  );
};

const App = () => {
  const [participants, setParticipants] = useState([]);
  const [loading, setLoading] = useState(true);
  const [isLive, setIsLive] = useState(false);
  const [lastUpdate, setLastUpdate] = useState(null);
  const [error, setError] = useState(null);

  useEffect(() => {
    // Real-time Firestore listener
    const q = query(
      collection(db, "leaderboard"), 
      orderBy("completed_count", "desc")
    );

    const unsubscribe = onSnapshot(
      q,
      (snapshot) => {
        const data = snapshot.docs.map((doc) => doc.data());
        setParticipants(data);
        setLastUpdate(new Date().toLocaleString());
        setIsLive(true);
        setLoading(false);
        setError(null);
      },
      (err) => {
        console.error("Firestore error:", err);
        setError(err.message);
        setLoading(false);
        setIsLive(false);
      }
    );

    // Cleanup
    return () => unsubscribe();
  }, []);

  return (
    <div className="min-h-screen bg-gradient-to-b from-gray-900 via-gray-900 to-black text-white">
      {/* Header */}
      <div className="bg-gradient-to-r from-purple-900/30 to-pink-900/30 border-b border-gray-800">
        <div className="max-w-6xl mx-auto px-6 py-8">
          <h1 className="text-5xl font-extrabold text-transparent bg-clip-text bg-gradient-to-r from-pink-500 via-purple-500 to-blue-500 text-center mb-2">
            Google Cloud Study Jams
          </h1>
          <p className="text-center text-gray-400 text-lg">
            Live Leaderboard - Real-time Updates
          </p>
        </div>
      </div>

      {/* Main Content */}
      <div className="max-w-6xl mx-auto px-6 py-8">
        {/* Error Banner */}
        {error && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            className="mb-6 bg-red-900/30 border border-red-500 rounded-xl p-4"
          >
            <div className="flex items-center gap-2 text-red-400">
              <AlertCircle className="w-5 h-5" />
              <span>Error: {error}</span>
            </div>
          </motion.div>
        )}

        {/* Loading State */}
        {loading ? (
          <div className="flex flex-col items-center justify-center py-20">
            <Loader2 className="w-12 h-12 animate-spin text-pink-500 mb-4" />
            <p className="text-gray-400">Connecting to live leaderboard...</p>
          </div>
        ) : (
          <Leaderboard 
            participants={participants} 
            lastUpdate={lastUpdate}
            isLive={isLive}
          />
        )}
      </div>

      {/* Footer */}
      <div className="mt-12 pb-8 text-center text-gray-500 text-sm">
        <p>Google Developers Group • Study Jams 2024</p>
        <p className="mt-1">
          {isLive ? (
            <span className="text-green-400 flex items-center justify-center gap-2">
              <span className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></span>
              Real-time updates enabled • Refreshes automatically
            </span>
          ) : (
            "Connecting to real-time updates..."
          )}
        </p>
      </div>
    </div>
  );
};

export default App;