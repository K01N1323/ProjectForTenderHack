import React, { useState, useEffect } from 'react';
import { Product } from '../../types';
import { useStore } from '../../store/store';
import { ShoppingCart, Star, X } from 'lucide-react';

interface ProductCardProps {
    product: Product;
}

const ProductCard: React.FC<ProductCardProps> = ({ product }) => {
    const [isOpen, setIsOpen] = useState(false);
    const { simulateProductOpen, simulateProductClose } = useStore();

    const handleOpen = () => {
        setIsOpen(true);
        simulateProductOpen(product.id, product.category);
    };

    const handleClose = (e: React.MouseEvent) => {
        e.stopPropagation();
        setIsOpen(false);
        simulateProductClose(product.id, product.category);
    };

    // Cleanup in case of unmount while open
    useEffect(() => {
        return () => {
            if (isOpen) {
                simulateProductClose(product.id, product.category);
            }
        };
    }, [isOpen, product.id, product.category, simulateProductClose]);

    return (
        <>
            <div 
                onClick={handleOpen}
                className="bg-white rounded-[8px] p-5 flex flex-col hover:shadow-md transition-shadow cursor-pointer relative h-full border border-gray-100 group"
            >
                {/* Personalization badge */}
                {product.reasonToShow && (
                    <div className="absolute -top-3 -right-3 bg-blue-50 text-blue-700 border border-blue-100 text-xs font-semibold px-3 py-1.5 rounded-full shadow-sm flex items-center gap-1 z-10">
                        ✨ {product.reasonToShow}
                    </div>
                )}
                
                <div className="text-xs text-gray-500 mb-2 uppercase tracking-wide truncate">{product.category}</div>
                
                <h3 className="text-gray-900 font-bold text-base md:text-lg mb-4 line-clamp-2 min-h-[50px] leading-snug group-hover:text-[#E03F3F] transition-colors">
                    {product.name}
                </h3>
                
                <div className="text-xs text-gray-400 mb-6">ИНН продавца: {product.supplierInn}</div>
                
                <div className="flex justify-between items-end mt-auto pt-4 border-t border-gray-50">
                    <div className="text-red-600 font-bold text-xl md:text-2xl leading-none">
                        {product.price.toLocaleString('ru-RU')} ₽
                    </div>
                    <button 
                        onClick={(e) => { e.stopPropagation(); /* Add to cart logic */ }}
                        className="bg-[#E03F3F] hover:bg-red-700 text-white rounded-[4px] w-12 h-12 flex items-center justify-center transition-colors shadow-sm focus:outline-none focus:ring-2 focus:ring-red-500 focus:ring-offset-2"
                    >
                        <ShoppingCart size={22} />
                    </button>
                </div>
            </div>

            {/* Simulated 'Opened Product' Modal */}
            {isOpen && (
                <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4 backdrop-blur-sm" onClick={handleClose}>
                    <div 
                        className="bg-white w-full max-w-lg rounded-[8px] p-8 relative flex flex-col shadow-2xl"
                        onClick={e => e.stopPropagation()}
                    >
                        <button onClick={handleClose} className="absolute top-4 right-4 text-gray-400 hover:text-gray-900 transition-colors">
                            <X size={28} />
                        </button>
                        <div className="text-sm text-gray-500 mb-2 uppercase tracking-wide">{product.category}</div>
                        <h2 className="text-2xl md:text-3xl font-bold mb-6 text-gray-900 leading-tight">{product.name}</h2>
                        
                        <div className="bg-blue-50 border border-blue-100 rounded p-4 mb-8 text-blue-800 text-sm">
                            <p className="font-semibold mb-1">Режим имитации пользовательского опыта</p>
                            Закройте это окно <strong>быстрее, чем за 3 секунды</strong>, чтобы система засчитала "Быстрый отказ". В результате эта категория товаров будет реже появляться в ленте и упадет в рейтинге.
                        </div>

                        <div className="text-red-600 font-bold text-3xl md:text-4xl">
                            {product.price.toLocaleString('ru-RU')} ₽
                        </div>
                    </div>
                </div>
            )}
        </>
    );
};

export default ProductCard;
