import { useState, useEffect, useRef, useCallback } from "react";
import { useThemeContext } from "../theme";
import { F } from "../config";

/**
 * VoiceInput — microphone button using Web Speech API.
 * Click to start listening, speak a command, result sent to onResult callback.
 * Shows waveform animation while listening.
 */
export default function VoiceInput({ onResult, lang = "fr-FR" }) {
  const C = useThemeContext();
  const [listening, setListening] = useState(false);
  const [transcript, setTranscript] = useState("");
  const [supported, setSupported] = useState(true);
  const recognitionRef = useRef(null);

  useEffect(() => {
    const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
    if (!SR) { setSupported(false); return; }

    const recognition = new SR();
    recognition.continuous = false;
    recognition.interimResults = true;
    recognition.lang = lang;
    recognition.maxAlternatives = 1;

    recognition.onresult = (event) => {
      let interim = "";
      let final = "";
      for (let i = event.resultIndex; i < event.results.length; i++) {
        const t = event.results[i][0].transcript;
        if (event.results[i].isFinal) final += t;
        else interim += t;
      }
      setTranscript(final || interim);
      if (final) {
        onResult?.(final.trim());
        setListening(false);
        setTranscript("");
      }
    };

    recognition.onerror = (event) => {
      console.warn("Speech error:", event.error);
      setListening(false);
      setTranscript("");
    };

    recognition.onend = () => {
      setListening(false);
    };

    recognitionRef.current = recognition;

    return () => {
      try { recognition.abort(); } catch {}
    };
  }, [lang, onResult]);

  const toggle = useCallback(() => {
    if (!recognitionRef.current) return;
    if (listening) {
      recognitionRef.current.stop();
      setListening(false);
    } else {
      setTranscript("");
      recognitionRef.current.start();
      setListening(true);
    }
  }, [listening]);

  if (!supported) return null;

  return (
    <button onClick={toggle} title={listening ? "Arreter l'ecoute" : "Commande vocale"}
      style={{
        fontFamily: F, fontSize: 11, padding: "6px 8px", borderRadius: 8,
        background: listening ? C.red + "20" : "transparent",
        border: `0.5px solid ${listening ? C.red + "55" : C.bdr}`,
        color: listening ? C.red : C.dim,
        cursor: "pointer", display: "flex", alignItems: "center", flexShrink: 0,
        animation: listening ? "pulse 1.5s ease-in-out infinite" : "none",
      }}>
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
        <path d="M12 1a3 3 0 0 0-3 3v8a3 3 0 0 0 6 0V4a3 3 0 0 0-3-3z" />
        <path d="M19 10v2a7 7 0 0 1-14 0v-2" />
        <line x1="12" y1="19" x2="12" y2="23" />
        <line x1="8" y1="23" x2="16" y2="23" />
      </svg>
    </button>
  );
}
