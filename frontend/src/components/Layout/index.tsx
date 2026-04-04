import React, { useState } from 'react';
import { useStore } from '../../store/store';
import { User as UserIcon, LogOut, ShoppingCart } from 'lucide-react';
import { useNavigate, useLocation } from 'react-router-dom';
import Cart from '../Cart';

const Layout = ({ children }: { children: React.ReactNode }) => {
    const { user, logout, cartProducts } = useStore();
    const [isCartOpen, setIsCartOpen] = useState(false);
    const navigate = useNavigate();
    const location = useLocation();

    const handleLogout = () => {
        logout();
        navigate('/login');
    };

    return (
        <div className="min-h-screen bg-gray-50 font-sans text-gray-900 flex flex-col">
            <header className="bg-white shadow-sm sticky top-0 z-40">
                <div className="max-w-[1400px] mx-auto px-6 h-24 flex items-center justify-between">
                    <div className="flex items-center cursor-pointer" onClick={() => navigate('/')}>
                        <img src="/logo.png" alt="Портал Поставщиков" className="h-20 object-contain" />
                    </div>

                    <div className="flex items-center gap-6">
                        {user ? (
                            <div className="flex items-center gap-4">
                                <div className="text-sm text-right hidden md:block">
                                    <div className="font-semibold text-gray-900">ООО "Ромашка" / ИНН {user.inn}</div>
                                </div>
                                <div className="w-9 h-9 bg-gray-100 rounded-full flex items-center justify-center text-gray-600">
                                    <UserIcon size={18} />
                                </div>
                                
                                <button 
                                    onClick={() => setIsCartOpen(true)}
                                    className="relative p-2 text-gray-600 hover:text-[#da291c] transition-colors"
                                    title="Корзина"
                                >
                                    <ShoppingCart size={24} />
                                    {cartProducts.length > 0 && (
                                        <span className="absolute -top-1 -right-1 bg-[#da291c] text-white text-[10px] font-bold w-5 h-5 flex items-center justify-center rounded-full border-2 border-white shadow-sm">
                                            {cartProducts.length}
                                        </span>
                                    )}
                                </button>

                                <button 
                                    onClick={handleLogout}
                                    className="text-gray-500 hover:text-[#da291c] transition-colors ml-2"
                                    title="Выход"
                                >
                                    <LogOut size={20} />
                                </button>
                            </div>
                        ) : (
                            <div className="flex items-center gap-6">
                                <button 
                                    onClick={() => setIsCartOpen(true)}
                                    className="relative p-2 text-gray-600 hover:text-[#da291c] transition-colors mr-2"
                                    title="Корзина"
                                >
                                    <ShoppingCart size={24} />
                                    {cartProducts.length > 0 && (
                                        <span className="absolute -top-1 -right-1 bg-[#da291c] text-white text-[10px] font-bold w-5 h-5 flex items-center justify-center rounded-full border-2 border-white shadow-sm">
                                            {cartProducts.length}
                                        </span>
                                    )}
                                </button>
                                {location.pathname !== '/login' && (
                                    <>
                                        <button 
                                            onClick={() => navigate('/login')}
                                            className="text-gray-500 font-bold hover:text-gray-700 transition-colors"
                                        >
                                            Войти
                                        </button>
                                        <button 
                                            onClick={() => navigate('/login')}
                                            className="bg-[#da291c] text-white px-6 py-2.5 font-bold hover:bg-red-700 transition-colors"
                                        >
                                            Зарегистрироваться
                                        </button>
                                    </>
                                )}
                            </div>
                        )}
                    </div>
                </div>
            </header>

            <Cart isOpen={isCartOpen} onClose={() => setIsCartOpen(false)} />
            
            <main className="flex-grow w-full relative">
                {children}
            </main>
        </div>
    );
};

export default Layout;
