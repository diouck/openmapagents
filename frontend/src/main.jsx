import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import App from "./App";
import DocIndex from "./components/doc/DocIndex";
import DocArticle from "./components/doc/DocArticle";
import { AuthProvider } from "./useAuth";

createRoot(document.getElementById("root")).render(
  <StrictMode>
    <AuthProvider>
      <BrowserRouter>
        <Routes>
          <Route path="/doc" element={<DocIndex />} />
          <Route path="/doc/:slug" element={<DocArticle />} />
          <Route path="*" element={<App />} />
        </Routes>
      </BrowserRouter>
    </AuthProvider>
  </StrictMode>
);
