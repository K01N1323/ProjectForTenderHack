import React from 'react';

const ProductCard = () => {
    return (
        <div className="product-card" style={{ 
            border: '1px solid #eee', 
            borderRadius: '12px', 
            padding: '20px', 
            position: 'relative',
            backgroundColor: '#fff',
            boxShadow: '0 4px 12px rgba(0,0,0,0.05)'
        }}>
            <h3 style={{ marginTop: 0 }}>Наименование СТЕ</h3>
            <p style={{ color: '#666' }}>Цена: 0.00 руб.</p>
            
            <div style={{ 
                backgroundColor: '#e3f2fd', 
                color: '#1976d2', 
                padding: '4px 8px', 
                borderRadius: '4px', 
                fontSize: '0.8rem',
                display: 'inline-block',
                marginTop: '10px'
            }}>
                Почему мы это показали (СТЕ-рекомендация)
            </div>
        </div>
    );
};

export default ProductCard;
