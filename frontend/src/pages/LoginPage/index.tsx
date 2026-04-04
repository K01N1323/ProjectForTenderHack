import React, { useState } from 'react';
import { useStore } from '../../store/store';
import { login } from '../../api';
import { useNavigate } from 'react-router-dom';
import { Loader2 } from 'lucide-react';

const LoginPage = () => {
    const [inn, setInnInput] = useState('');
    const [isLoading, setIsLoading] = useState(false);
    const { setUser } = useStore();
    const navigate = useNavigate();

    const handleLogin = async (e: React.FormEvent) => {
        e.preventDefault();
        if (!inn.trim()) return;

        setIsLoading(true);
        try {
            const fakeUser = await login(inn);
            setUser(fakeUser);
            navigate('/');
        } catch (error) {
            console.error("Login failed", error);
        } finally {
            setIsLoading(false);
        }
    };

    return (
        <div className="flex items-center justify-center min-h-[calc(100vh-64px)]">
            <div className="bg-white p-10 md:p-12 w-full max-w-[440px] border border-gray-200 shadow-sm">
                <div className="text-center mb-10">
                    <img src="/logo.png" alt="Портал Поставщиков" className="h-14 mx-auto mb-6 object-contain" />
                    <h1 className="text-2xl font-bold text-gray-900 tracking-tight">Вход в систему</h1>
                    <p className="text-gray-500 mt-2">Портал Поставщиков B2B</p>
                </div>

                <form onSubmit={handleLogin} className="flex flex-col gap-6">
                    <div>
                        <label className="block text-sm font-semibold text-gray-700 mb-2">ИНН организации или пользователя</label>
                        <input 
                            type="text" 
                            placeholder="Например, 7712345678" 
                            value={inn}
                            onChange={(e) => setInnInput(e.target.value)}
                            required
                            className="w-full px-5 py-3.5 border border-gray-300 focus:border-[#da291c] focus:ring-2 focus:ring-[#da291c33] outline-none transition-all text-base text-gray-900 bg-[#ecedf0] placeholder-gray-500"
                        />
                    </div>
                    
                    <div className="flex items-center justify-center gap-6 pt-2">
                        <button 
                            type="submit"
                            disabled={isLoading || !inn.trim()}
                            className="text-gray-500 font-bold text-lg hover:text-gray-700 transition-colors disabled:opacity-50"
                        >
                            {isLoading ? <Loader2 size={24} className="animate-spin" /> : "Войти"}
                        </button>
                        <button 
                            type="button"
                            className="bg-[#da291c] hover:bg-red-700 text-white font-bold py-3.5 px-8 transition-colors text-lg"
                        >
                            Зарегистрироваться
                        </button>
                    </div>
                </form>
            </div>
        </div>
    );
};

export default LoginPage;
