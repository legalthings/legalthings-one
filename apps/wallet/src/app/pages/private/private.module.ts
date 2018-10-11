import { NgModule } from '@angular/core';
import { SharedModule } from '../../shared';
import { RouterModule } from '@angular/router';
import { MatToolbarModule, MatButtonModule } from '@angular/material';
import { FlexLayoutModule } from '@angular/flex-layout';
import { PrivateComponent } from './private.component';

@NgModule({
  imports: [
    SharedModule,

    RouterModule.forChild([
      {
        path: '',
        component: PrivateComponent,
        children: [
          {
            path: 'portfolio',
            loadChildren: './portfolio-page/portfolio-page.module#PortfolioPageModule'
          },
          {
            path: 'transfers',
            loadChildren: './transactions/transactions.module#TransactionsModule'
          },
          {
            path: 'leasing',
            loadChildren: './leasing/leasing.module#LeasingModule'
          },
          {
            path: 'anchors',
            loadChildren: './anchors-page/anchors-page.module#AnchorsPageModule'
          },
          {
            path: '',
            redirectTo: 'transfers'
          }
        ]
      }
    ])
  ],
  declarations: [PrivateComponent],
  providers: []
})
export class PrivateModule {}