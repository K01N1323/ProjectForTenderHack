import React, { useState, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { Product } from '../../types';
import { useStore } from '../../store/store';
import { ChevronDown, ChevronUp, X, ShoppingCart } from 'lucide-react';

interface ProductCardProps {
    product: Product;
}

const ProductCard: React.FC<ProductCardProps> = ({ product }) => {
    const [isOpen, setIsOpen] = useState(false);
    const [showChars, setShowChars] = useState(false);
    const { simulateProductOpen, simulateProductClose, addToCart } = useStore();

    const handleOpen = () => {
        setIsOpen(true);
        simulateProductOpen(product.id, product.category);
    };

    const handleClose = (e: React.MouseEvent) => {
        e.stopPropagation();
        setIsOpen(false);
        simulateProductClose(product.id, product.category);
    };

    useEffect(() => {
        if (!isOpen) return;
        const previousOverflow = document.body.style.overflow;
        document.body.style.overflow = 'hidden';
        return () => {
            document.body.style.overflow = previousOverflow;
        };
    }, [isOpen]);

    return (
        <>
            <div 
                onClick={handleOpen}
                className="bg-white border border-gray-200 flex flex-col cursor-pointer relative h-full group hover:shadow-lg transition-shadow"
            >


                {/* Content */}
                <div className="p-5 flex flex-col flex-grow">
                    {/* Product name */}
                    <h3 className="text-[#333] font-bold text-base mb-4 line-clamp-3 min-h-[60px] leading-snug">
                        {product.name}
                    </h3>

                    {/* ID СТЕ */}
                    <div className="text-sm mb-1.5">
                        <span className="font-bold text-gray-700">ID СТЕ </span>
                        <span className="text-blue-600 underline decoration-dotted cursor-pointer">{product.id}</span>
                    </div>

                    {/* Производитель */}
                    <div className="text-sm mb-1.5">
                        <span className="font-bold text-gray-700">Производитель: </span>
                        <span className="text-blue-600 cursor-pointer">{product.supplierInn}</span>
                    </div>

                    {/* Категория */}
                    <div className="text-sm mb-4">
                        <span className="font-bold text-gray-700">Категория: </span>
                        <span className="text-blue-600 cursor-pointer">{product.category}</span>
                    </div>

                    {/* Characteristics toggle */}
                    <button 
                        onClick={(e) => {
                            e.stopPropagation();
                            setShowChars(!showChars);
                        }}
                        className="text-sm text-gray-700 font-semibold flex items-center gap-1 mb-4 underline decoration-dotted hover:text-blue-600 transition-colors"
                    >
                        Характеристики {showChars ? <ChevronUp size={16}/> : <ChevronDown size={16}/>}
                    </button>

                    {showChars && product.descriptionPreview && (
                        <div className="text-xs text-gray-600 mb-4 bg-gray-50 p-3 border border-gray-100">
                            {product.descriptionPreview}
                        </div>
                    )}

                    {/* Spacer */}
                    <div className="flex-grow" />

                    {/* Bottom section */}
                    <div className="border-t border-gray-200 pt-4 mt-2 flex items-end justify-between">
                        <div>
                            <div className="text-lg font-bold text-gray-900">
                                {product.price > 0 ? `${product.price.toLocaleString('ru-RU')} ₽` : '0'}
                            </div>
                            <div className="text-sm text-gray-500">Штука</div>
                        </div>
                        <div className="flex flex-col items-end gap-2">
                            <div className="text-sm text-gray-500 italic">
                                {product.price > 0 ? `${product.price.toLocaleString('ru-RU')} ₽` : 'Нет предложений'}
                            </div>
                            <button
                                onClick={(e) => {
                                    e.stopPropagation();
                                    addToCart(product);
                                }}
                                className="p-2 border border-[#da291c] text-[#da291c] hover:bg-red-50 transition-colors rounded"
                                title="Добавить в корзину"
                            >
                                <ShoppingCart size={18} />
                            </button>
                        </div>
                    </div>
                </div>

                {/* Personalization badge */}
                {product.reasonToShow && (
                    <div className="absolute top-2 right-2 bg-blue-50 text-blue-700 border border-blue-100 text-xs font-semibold px-2.5 py-1 shadow-sm flex items-center gap-1 z-10">
                        ✨ {product.reasonToShow}
                    </div>
                )}
            </div>

            {/* Simulated 'Opened Product' Modal */}
            {isOpen && typeof document !== 'undefined' && createPortal(
                <div className="fixed inset-0 bg-black/60 flex items-center justify-center z-[9999] p-4 backdrop-blur-sm" onClick={handleClose}>
                    <div 
                        className="bg-white w-full max-w-lg p-8 relative flex flex-col shadow-2xl"
                        onClick={e => e.stopPropagation()}
                    >
                        <button onClick={handleClose} className="absolute top-4 right-4 text-gray-400 hover:text-gray-900 transition-colors">
                            <X size={28} />
                        </button>
                        <div className="text-sm text-gray-500 mb-2 uppercase tracking-wide">{product.category}</div>
                        <h2 className="text-2xl md:text-3xl font-bold mb-6 text-gray-900 leading-tight">{product.name}</h2>

                        {product.descriptionPreview && (
                            <div className="bg-gray-50 border border-gray-200 p-4 mb-6 text-gray-700 text-sm leading-relaxed">
                                {product.descriptionPreview}
                            </div>
                        )}
                        
                        <div className="bg-blue-50 border border-blue-100 p-4 mb-8 text-blue-800 text-sm">
                            <p className="font-semibold mb-1">Режим имитации пользовательского опыта</p>
                            Закройте это окно <strong>быстрее, чем за 2 секунды</strong>, чтобы система засчитала "Быстрый отказ". В результате эта категория товаров будет реже появляться в ленте и упадет в рейтинге.
                        </div>

                        <div className="flex items-end justify-between gap-4">
                            <div className="text-[#1f2937] font-bold text-3xl md:text-4xl">
                                {product.price.toLocaleString('ru-RU')} ₽
                            </div>
                            <button
                                type="button"
                                onClick={(e) => {
                                    e.stopPropagation();
                                    addToCart(product);
                                }}
                                className="inline-flex items-center gap-2 bg-[#da291c] hover:bg-[#bf2418] text-white px-5 py-3 font-semibold transition-colors"
                            >
                                <ShoppingCart size={18} />
                                Добавить в корзину
                            </button>
                        </div>
                    </div>
                </div>,
                document.body
            )}
        </>
    );
};

export default ProductCard;
