import React, { useEffect, useRef, useState } from 'react';
import { useStore } from '../../store/store';
import { User as UserIcon, LogOut, ChevronDown, ShoppingCart } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import Cart from '../Cart';
import portalSuppliersLogo from '../../assets/portal-suppliers-logo.jpeg';

const Layout = ({ children }: { children: React.ReactNode }) => {
    const { user, logout, cartProducts } = useStore();
    const navigate = useNavigate();
    const [isProfileOpen, setIsProfileOpen] = useState(false);
    const [isCartOpen, setIsCartOpen] = useState(false);
    const profileRef = useRef<HTMLDivElement | null>(null);

    const handleLogout = () => {
        logout();
        navigate('/login');
    };

    useEffect(() => {
        if (!isProfileOpen) {
            return;
        }

        const handleClickOutside = (event: MouseEvent) => {
            if (!profileRef.current?.contains(event.target as Node)) {
                setIsProfileOpen(false);
            }
        };

        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, [isProfileOpen]);

    return (
        <div className="min-h-screen bg-[#f6f7f9] font-sans text-gray-900 flex flex-col">
            <header className="bg-white border-b border-[#eceef1] sticky top-0 z-40">
                <div className="max-w-[1600px] mx-auto px-8 lg:px-12 h-[108px] flex items-center justify-between">
                    <button
                        type="button"
                        className="flex items-center gap-5"
                        onClick={() => navigate('/')}
                    >
                        <img
                            src={portalSuppliersLogo}
                            alt="Портал поставщиков"
                            className="h-[72px] md:h-[80px] w-auto object-contain"
                        />
                    </button>

                    <div className="flex items-center gap-5">
                        {user ? (
                            <div className="flex items-center gap-4">
                                <div className="relative" ref={profileRef}>
                                    <button
                                        onClick={() => setIsProfileOpen((value) => !value)}
                                        className="flex items-center gap-3 rounded-[2px] border border-[#e6e8ec] bg-white px-4 py-2.5 hover:border-[#d9dde3] transition-colors"
                                    >
                                        <div className="text-sm text-right hidden md:block">
                                            <div className="font-semibold text-gray-900">Профиль заказчика / ИНН {user.inn}</div>
                                            <div className="text-xs text-gray-500">{user.region || 'Регион не определен'}</div>
                                        </div>
                                        <div className="w-9 h-9 bg-gray-100 rounded-full flex items-center justify-center text-gray-600">
                                            <UserIcon size={18} />
                                        </div>
                                        <ChevronDown size={16} className={`text-gray-400 transition-transform ${isProfileOpen ? 'rotate-180' : ''}`} />
                                    </button>

                                    {isProfileOpen && (
                                        <div className="absolute right-0 mt-3 w-[420px] max-w-[calc(100vw-32px)] max-h-[min(78vh,680px)] bg-white border border-gray-200 rounded-[8px] shadow-xl p-5 z-50 overflow-y-auto">
                                            <div className="mb-5">
                                                <div className="text-sm font-semibold text-gray-900">История закупок</div>
                                                <div className="text-xs text-gray-500 mt-1">
                                                    Персонализация поиска строится по истории ИНН {user.inn}
                                                </div>
                                            </div>

                                            <div className="mb-5">
                                                <div className="text-xs uppercase tracking-wide text-gray-400 mb-2">Топ-категории</div>
                                                <div className="flex flex-wrap gap-2">
                                                    {(user.topCategories ?? []).slice(0, 5).map((item) => (
                                                        <div key={item.category} className="px-3 py-2 rounded-full bg-[#fff5f5] border border-[#ffd6d6] text-sm text-gray-800">
                                                            <span className="font-semibold">{item.category}</span>
                                                            <span className="ml-2 text-gray-500">{item.purchaseCount}</span>
                                                        </div>
                                                    ))}
                                                    {(user.topCategories ?? []).length === 0 && (
                                                        <div className="text-sm text-gray-500">Категории не найдены.</div>
                                                    )}
                                                </div>
                                            </div>

                                            <div>
                                                <div className="text-xs uppercase tracking-wide text-gray-400 mb-2">Часто закупалось</div>
                                                <div className="space-y-3 pr-1">
                                                    {(user.frequentProducts ?? []).slice(0, 6).map((item) => (
                                                        <div key={item.steId} className="pb-3 border-b border-gray-100 last:border-b-0 last:pb-0">
                                                            <div className="font-semibold text-gray-900 leading-snug">{item.name}</div>
                                                            <div className="text-sm text-gray-500 mt-1">{item.category}</div>
                                                            <div className="text-xs text-gray-400 mt-1">Закупок: {item.purchaseCount}</div>
                                                        </div>
                                                    ))}
                                                    {(user.frequentProducts ?? []).length === 0 && (
                                                        <div className="text-sm text-gray-500">История закупок для этого ИНН пока не найдена.</div>
                                                    )}
                                                </div>
                                            </div>
                                        </div>
                                    )}
                                </div>

                                <button
                                    type="button"
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
                                    className="text-gray-500 hover:text-[#d63d2b] transition-colors"
                                    title="Выход"
                                >
                                    <LogOut size={20} />
                                </button>
                            </div>
                        ) : (
                            <div className="flex items-center gap-8">
                                <button
                                    type="button"
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
                                    type="button"
                                    onClick={() => navigate('/login')}
                                    className="text-[18px] font-bold text-[#6b7280] hover:text-[#404755] transition-colors"
                                >
                                    Войти
                                </button>
                                <button
                                    type="button"
                                    onClick={() => navigate('/login')}
                                    className="bg-[#d63d2b] hover:bg-[#bf3324] text-white font-bold text-[18px] px-8 py-3 rounded-[2px] transition-colors shadow-sm"
                                >
                                    Зарегистрироваться
                                </button>
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
