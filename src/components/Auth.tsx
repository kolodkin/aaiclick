import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from "react";
import { clearSession, fetchMe, logout as doLogout, type MeView } from "../lib/auth";

interface AuthState {
  me: MeView | null;
  ready: boolean; // false until the initial /auth/me probe resolves
  // Mutating controls are admin-only server-side; the UI disables rather than
  // hides them so a viewer can see the action exists. When the server has auth
  // disabled, /auth/me returns a synthetic admin, so nothing is gated.
  isAdmin: boolean;
  refresh: () => Promise<void>;
  signOut: () => Promise<void>;
}

const AuthContext = createContext<AuthState>({
  me: null,
  ready: false,
  isAdmin: false,
  refresh: async () => {},
  signOut: async () => {},
});

export function useAuth(): AuthState {
  return useContext(AuthContext);
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [me, setMe] = useState<MeView | null>(null);
  const [ready, setReady] = useState(false);

  const refresh = useCallback(async () => {
    setMe(await fetchMe());
  }, []);

  const signOut = useCallback(async () => {
    await doLogout();
    setMe(null);
  }, []);

  useEffect(() => {
    void refresh().finally(() => setReady(true));
  }, [refresh]);

  // The API client dispatches this when a request fails auth even after a
  // refresh attempt — drop back to the login screen.
  useEffect(() => {
    const onUnauthorized = () => {
      clearSession();
      setMe(null);
    };
    window.addEventListener("aaiclick:unauthorized", onUnauthorized);
    return () => window.removeEventListener("aaiclick:unauthorized", onUnauthorized);
  }, []);

  return (
    <AuthContext.Provider value={{ me, ready, isAdmin: me?.superadmin ?? false, refresh, signOut }}>
      {children}
    </AuthContext.Provider>
  );
}
