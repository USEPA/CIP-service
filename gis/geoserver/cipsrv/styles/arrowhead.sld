<?xml version="1.0" encoding="ISO-8859-1"?>
<StyledLayerDescriptor version="1.0.0" 
    xsi:schemaLocation="http://www.opengis.net/sld StyledLayerDescriptor.xsd" 
    xmlns="http://www.opengis.net/sld" 
    xmlns:ogc="http://www.opengis.net/ogc" 
    xmlns:xlink="http://www.w3.org/1999/xlink" 
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
   <NamedLayer>
      <Name>FlowDirection</Name>
      <UserStyle>
         <Title>FlowDirection</Title>
         <FeatureTypeStyle>
            <Rule>
               <MaxScaleDenominator>500000</MaxScaleDenominator>
               <PointSymbolizer>
                  <Graphic>
                     <Mark>
                        <WellKnownName>wkt://POLYGON((0 0, -2 -4, 0 -3, 2 -4, 0 0))</WellKnownName>
                        <Fill>
                           <CssParameter name="fill">#000000</CssParameter>
                        </Fill>
                     </Mark>
                     <Size>10</Size>
                     <Rotation>
                        <ogc:PropertyName>angle_rounded</ogc:PropertyName>
                     </Rotation>
                  </Graphic>
               </PointSymbolizer>
            </Rule>
         </FeatureTypeStyle>
      </UserStyle>
   </NamedLayer>
</StyledLayerDescriptor>
