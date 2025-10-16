import { initializeApp } from "firebase/app";
import { getFirestore, collection, query, orderBy, onSnapshot } from "firebase/firestore";

const firebaseConfig = {
  apiKey: "AIzaSyC4QDo4v-fcJLUN85VmnhYFT5kiYuocrIg",
  authDomain: "study-jam-leaderboard.firebaseapp.com",
  projectId: "study-jam-leaderboard",
  storageBucket: "study-jam-leaderboard.firebasestorage.app",
  messagingSenderId: "608271611519",
  appId: "1:608271611519:web:6ee95a6871dfdfa949f14a"
};

const app = initializeApp(firebaseConfig);
const db = getFirestore(app);

export { db, collection, query, orderBy, onSnapshot };
